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
      } catch (Exception $e) {
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

    $errors = $this->getErrors($response, $result);

    if ($this->isError($errors)) {
      throw new CakeException($this->errorString($errors));
    } else {
      return true;
    }
  }

  private function basicAuth() {
    $this->Socket->config['request']['uri']['user'] = $this->config['login'];
    $this->Socket->config['request']['uri']['pass'] = $this->config['password'];

    $response = $this->Socket->get('/');
    $result = json_decode($response->body(), true);

    $errors = $this->getErrors($response, $result);
    /* http://wiki.apache.org/couchdb/HttpGetRoot
     * gotta be careful, since result can be set to anything, even {"error": "hi"},
     * false or null
     *
     * so don't do that ;)
     */

    if ($this->isError($errors)) {
      throw new CakeException($this->errorString($errors));
    } else {
      return true;
    }
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

    $data = json_encode($data);

    //TODO: remove switch, validate method against an array and use request on all

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

      // http://wiki.apache.org/couchdb/HTTP_Document_API#HEAD
      // to fetch revision
      case 'head':
        $response = $this->Socket->request(array('method' => 'HEAD', 'uri' => $url, 'body' => $data));
        break;

      case 'get':
      default:
        $response = $this->Socket->get($url, $data);
        break;
    }

    $t = round((microtime(true) - $t) * 1000, 0);

    $result = json_decode($response->body(), true);

    $errors = $this->getErrors($response, $result);

    if ($this->logQueries && (count($this->_queriesLog) <= $this->_queriesLogMax)) {
      $this->_queriesCnt++;
      $this->_queriesTime += $t;
      $this->_queriesLog[] = array(
        'query'   => $method . ' ' . $url . ' ' . print_r($data, true),
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

    return array('body' => $result, 'errors' => $errors, 'headers' => $response->headers);
  }

  private function getErrors(&$response, &$result) {
    $errors = array(
      'http'    => false,
      'couch' => false,
      'json'    => false
    );

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

    return $errors;
  }

  private function errorString($errors) {
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

  private function isError($errors) {
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

    if (in_array($model->primaryKey, array_keys($data))) {
      $id = $data[$model->primaryKey];
    }
    if ($model->id !== false) {
      $id = $model->id;
    }

    if ($id === false) {
      $id = String::uuid();
    }

    if (($this->config['models'] !== false) && !isset($data[$this->config['models']])) {
      $data[$this->config['models']] = strtolower($model->name);
    }

    $url = '/'. $this->getDB($model->database) . '/' . $id;

    $response = $this->query($url, 'put', $data);

    if (isset($response['body']['ok']) && ($response['body']['ok'] == true)) {

      $model->data = $data;

      $model->id = $response['body']['id'];
      $model->data[$model->primaryKey] = $response['body']['id'];

      $model->data[$model->revisionKey] = $response['body']['rev'];
      return $data;
    } else {
      $model->onError();
      return false;
    }
  }


  public function read(Model &$model, $queryData = array(), $recursive = null) {

    $url = '/' . $this->getDB($model->database) . '/';
    $params = array();

    if (isset($queryData['view']) && isset($queryData['design'])) {

      $url .= '/_design/' . $queryData['design'] . '/_view/' . $queryData['view'];

    } elseif(isset($queryData['conditions'][$model->alias . '.' . $model->primaryKey])) {

      $url .= $queryData['conditions'][$model->alias . '.' . $model->primaryKey];

    } else {

      $url .= '_all_docs';

      if (!empty($queryData['limit'])) {

        $params['limit'] = $queryData['limit'];
        $params['descending'] = 'true';

        if (!empty($queryData['offset'])) {
          // see http://stackoverflow.com/questions/3924089/previous-link-equivalent-of-limit-x-offset-y
          // and http://guide.couchdb.org/draft/recipes.html#pagination
          // on this. it'll enable us to use a somewhat crippled cakephp native pagination later :) neat, eh?

          $params['startkey'] = json_encode($queryData['offset']);
          $params['skip'] = 1;
          // ? increase skip value for real pagination with page jumping? page * 10 e.g.?

        }
      }
    }

    if ($queryData['fields'] == 'count') {
      unset($queryData['params']['limit']);
    } else {
      $params['include_docs'] = 'true';
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
        $result[] = array($model->alias => $response['body']);
      }
    } else {

      if($queryData['fields'] == 'count') {
        // documents count is requested
        $result[] = array(
          $model->alias => array(
            'count' => count($response['body']['rows'])
          )
        );
      } else {
        // a collection of documents is requested
        if (isset($response['body']['rows']) && !empty($response['body']['rows'])){
          foreach($response['body']['rows'] as $row) {
            $result[] = array($model->alias => $row);
          }
        }
      }
    }

    return $result;
  }

//////////////////////////////////////////////////
  public function update(Model &$model, $fields = array(), $values = null, $conditions = null) {

    if ($values === null) {
      $data = $fields;
    } else {
      $data = array_combine($fields, $values);
    }

    $id = false;

    if (in_array($model->primaryKey, array_keys($data))) {
      $id = $data[$model->primaryKey];
    }
    if ($model->id !== false) {
      $id = $model->id;
    }

    if ($id === false) {
      return false;
    }

    $url = '/'. $this->getDB($model->database) . '/' . $id;

    if (!isset($data[$model->revisionKey])) {
      $response = $this->query($url, 'get');

      if ($this->isError($response['errors'])) {
        return false;
      } else {
        $data = array_merge($response['body'], $data);
        $data[$model->revisionKey] = $response['body'][$model->revisionKey];
      }
    }

    $response = $this->query($url, 'put', $data);

    if (isset($response['body']['ok']) && ($response['body']['ok'] == true)) {
      if (($this->config['models'] !== false) && !isset($data[$this->config['models']])) {
        unset($data[$this->config['models']]);
      }
      $model->data = $data;

      $model->id = $response['body']['id'];
      $model->data[$model->primaryKey] = $response['body']['id'];

      $model->data[$model->revisionKey] = $response['body']['rev'];
      return true;
    } else {
      $model->onError();
      return false;
    }
  }

  public function delete(Model $model, $conditions = null) {
    $id = false;
    $revision = false;

    if (in_array($model->alias . '.' . $model->primaryKey, array_keys($conditions))) {
      $id = $conditions[$model->alias . '.' . $model->primaryKey];
    } elseif ($model->id !== false) {
      $id = $model->id;
    }

    if (in_array($model->alias . '.' . $model->revisionKey, array_keys($conditions))) {
      $revision = $conditions[$model->alias . '.' . $model->revisionKey];
    } elseif (isset($model->data[$model->revisionKey])) {
      $revision = $model->data[$model->revisionKey];
    }

    if (($id === false) || ($revision === false)) {
      return false;
    }

    $url = '/'. $this->getDB($model->database) . '/' . $id . '?rev=' . $revision;
    $response = $this->query($url, 'delete');

    return !$this->isError($response['errors']) && ($response['body']['ok'] == true);
  }

  public function getRevision(Model $model, $id = false) {
    if (($id === false) && isset($model->data[$model->primaryKey])) {
      $id = $model->data[$model->primaryKey];
    }
    if ($id === false) {
      $id = $model->id;
    }
    if ($id === false) {
      return false;
    }

    $url = '/'. $this->getDB($model->database) . '/' . $id;

    // http://wiki.apache.org/couchdb/HTTP_Document_API#HEAD
    // to fetch revision
    $response = $this->query($url, 'head');

    if ( $this->isError($response['errors']) || (!isset($response['headers']['Etag'])) ) {
      return false;
    } else {
      return trim($response['headers']['Etag'], '"');
    }
  }

}
