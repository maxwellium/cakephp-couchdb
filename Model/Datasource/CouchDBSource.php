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
    'database'  => ''
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
    return $autoConnect ? $this->connect() : true;
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
    $this->config = array_replace_recursive($_baseconfig, $this->config, $config,$translatedConfig);
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
    $result = json_decode(
      $this->Socket->post(
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
      ),
      true
    );

    return (json_last_error() != JSON_ERROR_NONE) && isset($result['ok']) && ($result['ok'] == true);
  }

  private function basicAuth() {
    $this->Socket->config['request']['uri']['user'] = $this->config['user'];
    $this->Socket->config['request']['uri']['pass'] = $this->config['password'];

    $result = json_decode($this->Socket->get('/'), true);
    /* http://wiki.apache.org/couchdb/HttpGetRoot
     * gotta be careful, since this can be set to ""
     * don't do that ;)
     */

    return (json_last_error() != JSON_ERROR_NONE) && ($result !== null);
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



  public function describe(&$model) {
    return $model->_schema;
  }

  public function sources($reset = false) {
    if ($reset === true) {
      $this->_sources = null;
    }
    return array_map('strtolower', $this->listSources());
  }

  public function listSources() {
    if ($this->cacheSources === false) {
      return null;
    } elseif ($this->_sources !== null) {
      return $this->_sources;
    }

    $this->_sources = $this->execute(
      '/'.$this->config['database'].'/_temp_view?group=true',
      'post',
      array(
        'language'  => 'javascript',
        'map'       => 'function(doc) { if (doc.'. $this->config['models'] .') {emit(doc.'. $this->config['models'] .',1);}}',
        'reduce'    => 'function(keys, values) { return sum(values);}'
      )
    );

    // TODO:
    /* I consider the above solution bad practice.. I think about implementing this view in futon
     * and adding path to DATABASE_CONFIG
     * maybe just optional
     *
     * currently requires admin privileges
     * see http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference
     *  Database methods
     *    POST /db/_temp_view
     */

    return $this->_sources;
  }

  public function execute($url, $method = 'get', $data = '') {
    $t = microtime(true);

    $data = json_encode($data);

    switch ($method) {
      case 'post':
        $result = $this->Socket->post($url, $data);
        break;

      case 'put':
        $result = $this->Socket->put($url, $data);
        break;

      case 'delete':
        $result = $this->Socket->delete($url, $data);
        break;

      case 'get':
      default:
        $result = $this->Socket->get($url, $data);
        break;
    }

    $t = round((microtime(true) - $t) * 1000, 0);

    $result = json_decode($result, true);
    if (is_null($result)) {
      throw new CakeException(json_last_error());
    }

    if ($this->logQueries && (count($this->_queriesLog) <= $this->_queriesLogMax)) {
      $this->_queriesCnt++;
      $this->_queriesTime += $t;
      $this->_queriesLog[] = array(
        'query'   => $url,
        'params'  => $data,
        'affected'=> 0,
        'numRows' => 0,
        'took'    => $t
      );
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


  public function create(Model &$model, $fields = null, $values = null) {
  /*
   * http://wiki.apache.org/couchdb/HTTP_Document_API#POST
   * http://wiki.apache.org/couchdb/HTTP_Document_API#PUT
   *
   * see link on POST to understand why I generate uuid in here
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

    $url = sprintf(
      '/%s/%s',
      is_null($model->database) ? $this->config['database'] : $model->database,
      $id
    );

    $result = $this->execute($url, 'put', $data);

    if ($result['ok'] == true) {

      unset($data[$this->config['models']])
      $model->data = $data;

      $model->id = $result['id'];
      $model->data['id'] = $result['id'];

      $model->rev = $result['rev'];
      return true;
    }

    $model->onError();
    return false;
  }

  public function calculate($model, $func, $params = array()) {
    return 'COUNT';
  }



//////////////////////////////////////////////////
  public function read(Model &$model, $queryData = array()) {
    return false;
  }
  public function update(Model &$model, $fields = null, $values = null) {
    return false;
  }
  public function delete(Model &$model, $id = null) {
    return false;
  }

}
