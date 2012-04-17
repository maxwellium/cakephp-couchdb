<?php

App::uses('HttpSocket', 'Network/Http');

/**
* Assuming a connection as follows:
**/
/*
class DATABASE_CONFIG {

  // remember to update $useDbConfig in Model or CouchDBAppModel.php if you change this name!
  public $couchDB = array(
    'datasource' => 'CouchDB.CouchDBSource',

    'host'      => 'localhost', // optional
    'port'      => 5984,        // optional
    'login'     => 'root',
    'password'  => 'root',

    'models'    => 'type',      // (optional)
    'database'  => 'cake',      // can be overridden in Model->database
  );
}

*/

class CouchDBSource extends DataSource {

  /**
  * See the parent class DataSource http://api.cakephp.org/class/data-source
  * and http://api.cakephp.org/class/data-source#method-DataSourcesetConfig
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
     * gotta be careful, since this can be set to ""
     * don't do that ;)
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

  public function execute($url, $method = 'get', $data = array()) {
    $t = microtime(true);
    $e = '';

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
      $e .= ' HTTP: ' . $response->code . ' ' . $response->reasonPhrase . ';';

      if ((json_last_error() == JSON_ERROR_NONE) && is_array($result) && isset($result['error'])) {
        $e .= ' CouchDB: ' . $result['error'];
      }
    }
    if (json_last_error() != JSON_ERROR_NONE) {
      $e .= ' JSON Error: ' .json_last_error() .';';
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
        'error'   => ltrim($e)
      );
    }

    if ($e != '') {
      throw new CakeException($e);
    }

    return $result;
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

    if (in_array($model->primaryKey, $data)) {

      $id = $data[$model->primaryKey];
      unset($data[$model->primaryKey]);
    } else {

      $id = String::uuid();
    }
    $data[$this->config['models']] = strtolower($model->name);

    $url = '/'. $this->getDB($model->database) . '/' . $id;

    $result = $this->execute($url, 'put', $data);

    if ($result['ok'] == true) {

      unset($data[$this->config['models']]);
      $model->data = $data;

      $model->id = $result['id'];
      $model->data[$model->primaryKey] = $result['id'];

      $model->{$model->revisionKey} = $result['rev'];
      return $data;
    }

    $model->onError();
    return false;
  }

  public function calculate($model, $func, $params = array()) {
    return 'COUNT';
  }



//////////////////////////////////////////////////
  public function read(Model &$model, $queryData = array(), $recursive = null) {

    debug($queryData);

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
      }
    }

    if($queryData['fields'] == 'count') {
      unset($queryData['params']['limit']);
    }

    $url .= '?' . http_build_query(
      array_merge($params, isset($queryData['params']) ? $queryData['params'] : array()),
      '',
      '&' // to avoid problems on some server configurations with php5.3
    );    // see comment from 08-Feb-2011 12:11 on http://php.net/manual/de/function.http-build-query.php


    $rows = $this->execute($url);

    $result = array();

    if (isset($queryData['conditions'][$model->alias . '.' . $model->primaryKey])) {

      if ($queryData['fields'] == 'count') {
        $result[] = array(
          $model->alias => array('count' => 1)
        );
      } else {
        $result[] = array($model->alias => $rows);
      }
    } else {

      if($queryData['fields'] == 'count') {
        // documents count is requested
        $result[] = array(
          $model->alias => array(
            'count' => count($rows['rows'])
          )
        );
      } else {
        // a collection of documents is requested
        if (isset($rows['rows']) && !empty($rows['rows'])){
          foreach($rows['rows'] as $row) {
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
