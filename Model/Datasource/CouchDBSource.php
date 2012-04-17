<?php

App::uses('HttpSocket', 'Network/Http');


class CouchDBSource extends DataSource {

  /**
  * See the parent class DataSource http://api.cakephp.org/class/data-source
  * and http://api.cakephp.org/class/data-source#method-DataSourcesetConfig
  * on this convention
  **/
  protected $_baseconfig = array(
    'request'   => array(
      'uri' => array(
        'host' => 'localhost',
        'port' => 5984
      ),
      'header'  => array(
        'Content-Type' => 'application/json'
      )
    ),

    'authType'  => 'basic',
    'models'    => 'type',
    'database'  => null
  );

  public $logQueries = true;

  protected $_queriesLog = array();
  protected $_queriesCnt = 0;
  protected $_queriesTime = null;
  protected $_queriesLogMax = 200;

  /**
  * See parent http://api.cakephp.org/class/data-source#method-DataSource__construct
  **/
  public function __construct($config = null, $autoConnect = true){
    parent::__construct($config);
    if ($autoConnect) {
      $this->connect();
    }
  }

  public function setConfig($config = array()) {
    $translateConfig = array(
      'host'      => 'request.uri.host',
      'port'      => 'request.uri.port'
    );
    $translatedConfig = array();
    foreach ($config as $key => $value) {
      if (isset($translateConfig[$key])) {
        $translatedConfig = Set::insert($translatedConfig, $translateConfig[$key], $value);
        unset($config[$key]);
      }
    }
    $this->config = array_replace_recursive($this->_baseconfig, $this->config, $config,$translatedConfig);
  }

  /**
  * for HttpSocket see http://book.cakephp.org/2.0/en/core-utility-libraries/httpsocket.html
  **/
  public function connect() {
    if ($this->connected !== true) {
      try {
        $this->Socket = new HttpSocket($this->config);

        switch ($this->config['authType']) {
          case 'cookie':
            $this->connected = $this->cookieAuth();
            break;
          case 'oauth':
            $this->connected = false; //to be implemented later
            break;
          case 'basic':
          default:
            $this->connected = $this->basicAuth();
            break;
        }

        // ? throw error if still not connected
      } catch (SocketException $e) {
        throw new MissingConnectionException(array('class' => $e->getMessage()));
      }
    }
    return $this->connected;
  }

  private function cookieAuth() {
    // ? check for /_session first.. http://wiki.apache.org/couchdb/Session_API

    $response = $this->Socket->post(
      '/_session',
      array(
        'name' => $this->config['login'],
        'password' => $this->config['password']
      ),
      array(
        'header' => array(
          'Content-Type' => 'application/x-www-form-urlencoded'
        )
      )
    );

    $result = json_decode($response->body(), true);

    return ($response->code < 400) &&
      (json_last_error() != JSON_ERROR_NONE) &&
      isset($result['ok']) && ($result['ok'] == true);
  }

  private function basicAuth() {
    $this->Socket->config['request']['uri']['user'] = $this->config['login'];
    $this->Socket->config['request']['uri']['pass'] = $this->config['password'];

    $response = $this->Socket->get('/');
    $result = json_decode($response->body(), true);
    /* http://wiki.apache.org/couchdb/HttpGetRoot
     * gotta be careful, since this can be set to "", false or null
     * so don't do that ;)
     */

    return ($response->code < 400) && (json_last_error() != JSON_ERROR_NONE) && ($result !== null);
  }

  public function reconnect($config = null) {
    $this->disconnect();
    $this->setConfig($config);
    $this->_sources = null;
    return $this->connect();
  }

  public function disconnect() {
    if (isset($this->results) && is_resource($this->results)) {
      $this->results = null;
      $this->Socket->reset();
    }
    $this->connected = false;
    return !$this->connected;
  }

  public function close() {
    $this->disconnect();
  }

  public function query($url, $method = 'get', $data = array()) {
    $t = microtime(true);
    $errors = array(
      'http'    => false,
      'couch' => false,
      'json'    => false
    );

    $data = json_encode($data);

    switch ($method) {
      case 'post':
        $response = $this->Socket->post($url, $data);
        break;

      case 'put':
        $response = $this->Socket->put($url, $data);
        break;

      case 'delete':
        $response = $this->Socket->delete($url, $data);
        break;

      case 'get':
      default:
        $response = $this->Socket->get($url, $data);
        break;
    }

    $t = round((microtime(true) - $t) * 1000, 0);

    $result = json_decode($response->body(), true);

    // see http://guide.couchdb.org/draft/api.html on this test
    if ($response->code >= 400) {
      $errors['http'] = array(
        'code' => $response->code,
        'message' => $response->reasonPhrase
      );

      if ((json_last_error() == JSON_ERROR_NONE) && is_array($result) && isset($result['error'])) {
        $errors['couch'] = array('error' => $result['error']);
        if (isset($result['reason'])) {
          $errors['couch']['reason'] = $result['reason'];
        }
      }
    }
    if (json_last_error() != JSON_ERROR_NONE) {
      $errors['json'] = json_last_error();
    }

    if ($this->logQueries && (count($this->_queriesLog) <= $this->_queriesLogMax)) {
      $this->_queriesCnt++;
      $this->_queriesTime += $t;
      $this->_queriesLog[] = array(
        'query'   => $method . ' ' . $url,
        'params'  => $data,
        'affected'=> 0,
        'numRows' => 0,
        'took'    => $t,
        'error'   => $this->errorString($errors)
      );
    }

    // the reason we're only throwing json-errors, is that couchDB can of course respond
    // with a 404 when we're trying to find a document. since cakephp prefetches records
    // when an id is set, this would destroy update logic.
    if (($errors['json'] !== false) ||
      ($errors['couch']['error'] == 'unauthorized') ||
      (isset($errors['couch']['reason']) && ($errors['couch']['reason']) == 'no_db_file')) {

      throw new CakeException($this->errorString($errors));
    }

    return array('result' => $result, 'errors' => $errors);
  }

  public function errorString($errors) {
    $e = '';
    foreach ($errors as $type => $value) {
      if ($value !== false) {
        $e .= ' (' . $type . ')';
        if (is_array($value)) {
          foreach ($value as $info) {
            $e .= ' ' . $info;
          }
        } else {
          $e .= ' ' . $value;
        }
        $e .= ';';
      }
    }
    return ltrim($e);
  }

  public function isError($errors) {
    foreach ($errors as $error) {
      if ($error !== false) {
        return true;
      }
    }
    return false;
  }

  public function getLog($sorted = false, $clear = true) {
    if ($sorted) {
      $log = sortByKey($this->_queriesLog, 'took', 'desc', SORT_NUMERIC);
    } else {
      $log = $this->_queriesLog;
    }
    if ($clear) {
      $this->_queriesLog = array();
    }
    return array('log' => $log, 'count' => $this->_queriesCnt, 'time' => $this->_queriesTime);
  }


  public function getDB($modelDB = null) {
    // see http://wiki.apache.org/couchdb/HTTP_database_API
    // ? it doesn't tell whether $()+- need to be escaped as well..?
    return str_replace(
      '/',
      '%2F',
      is_null($modelDB) ? $this->config['database'] : $modelDB
    );
  }

  public function create(Model &$model, $fields = null, $values = null) {
  /*
   * see http://wiki.apache.org/couchdb/HTTP_Document_API#POST to understand why I generate uuid in here
   * http://wiki.apache.org/couchdb/HTTP_Document_API#PUT
   */

    $data = $model->data;

    if ($fields !== null && $values !== null) {
      $data = array_combine($fields, $values);
    }

    $id = false;

    if (in_array($model->primaryKey, $data)) {
      $id = $data[$model->primaryKey];
      unset($data[$model->primaryKey]);
    }

    if ($model->id !== false) {
      $id = $model->id;
    }

    if ($id === false) {
      $id = String::uuid();
    }

    if ($this->config['models'] !== false) {
      $data[$this->config['models']] = strtolower($model->name);
    }

    $url = '/'. $this->getDB($model->database) . '/' . $id;

    $response = $this->query($url, 'put', $data);

    if (isset($response['result']['ok']) && ($response['result']['ok'] == true)) {

      unset($data[$this->config['models']]);
      $model->data = $data;

      $model->id = $response['result']['id'];
      $model->data[$model->primaryKey] = $response['result']['id'];

      $model->{$model->revisionKey} = $response['result']['rev'];
      return $data;
    } else {
      $model->onError();
      return false;
    }
  }


//////////////////////////////////////////////////
  public function read(Model &$model, $queryData = array(), $recursive = null) {

    $url = '/' . $this->getDB($model->database) . '/';
    $params = array();

    if (isset($queryData['view']) && isset($queryData['design'])) {

      $url .= '/_design/' . $queryData['design'] . '/_view/' . $queryData['view'];

    } elseif(isset($queryData['conditions'][$model->alias . '.' . $model->primaryKey])) {

      $url .= $queryData['conditions'][$model->alias . '.' . $model->primaryKey];

    } else {

      $url .= '_all_docs';

      $params['include_docs'] = 'true';

      if (!empty($queryData['limit'])) {

        $params['limit'] = $queryData['limit'];
        $params['descending'] = 'true';

        if (!empty($queryData['offset'])) {
          // see http://stackoverflow.com/questions/3924089/previous-link-equivalent-of-limit-x-offset-y
          // and http://guide.couchdb.org/draft/recipes.html#pagination
          // on this. it'll enable us to use a somewhat crippled cakephp native pagination later :) neat, eh?

          $params['startkey'] = $queryData['offset'];
          $params['skip'] = 1;
          // ? increase skip value for real pagination with page jumping? page * 10 e.g.?

        }
      }
    }

    if($queryData['fields'] == 'count') {
      unset($queryData['params']['limit']);
    }

    $params = array_merge(
      $params,
      isset($queryData['params']) ? $queryData['params'] : array()
    );

    $response = $this->query($url, 'get', $params);

    if ($this->isError($response['errors'])) {
      return false;
    }

    $result = array();

    if (isset($queryData['conditions'][$model->alias . '.' . $model->primaryKey])) {

      if ($queryData['fields'] == 'count') {
        $result[] = array(
          $model->alias => array('count' => 1)
        );
      } else {
        $result[] = array($model->alias => $response['result']);
      }
    } else {

      if($queryData['fields'] == 'count') {
        // documents count is requested
        $result[] = array(
          $model->alias => array(
            'count' => count($response['result']['rows'])
          )
        );
      } else {
        // a collection of documents is requested
        if (isset($response['result']['rows']) && !empty($response['result']['rows'])){
          foreach($response['result']['rows'] as $row) {
            $result[] = array($model->alias => $row);
          }
        }
      }
    }

    return $result;
  }

  public function update(Model &$model, $fields = null, $values = null) {
    debug('update');
    debug($fields);
    debug($values);
    return array();
  }
  public function delete(Model &$model, $id = null) {
    debug('delete');
    debug($id);
    return false;
  }

}
