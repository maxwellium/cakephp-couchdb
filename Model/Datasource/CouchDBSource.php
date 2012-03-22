<?php

App::uses('HttpSocket', 'Network/Http');

/**
* Assuming a connection as follows:
**/
/*
class DATABASE_CONFIG {

  public $default = array(
    'host'      => 'localhost', // optional
    'port'      => 5984,        // optional
    'login'     => 'root',
    'password'  => 'root',

    'models'    => 'type',      // (optional)
    'database'  => 'cake',      // can be overridden in Model->database)

    'models'    => false        // if models are distinguished by database instead
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
  protected $_queriesLog = array();
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
            );
            break;
          case 'basic':
          default:
            $this->Socket->config['request']['uri']['user'] = $this->config['user'];
            $this->Socket->config['request']['uri']['pass'] = $this->config['password'];
            break;
         }
        }

        $this->connected = (json_decode($this->Socket->get('/'.$this->config['database'])) !== null) &&
          (json_last_error() == JSON_ERROR_NONE);

      } catch (SocketException $e) {
        throw new MissingConnectionException(array('class' => $e->getMessage()));
      }
    }
    return $this->connected;
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

  /**
  * (called by DataSource destructor)
  **/
  public function close() {
    $this->disconnect();
  }


//////////////////////////////////////////////////

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

    //needs to be changed to $this->config['models']
    if ($this->config['models'] === false) {
      $this->_sources = $this->execute('/_all_dbs');
    } else {
      $this->_sources = $this->execute(
        '/'.$this->config['database'].'/_temp_view?group=true',
        'post',
        array(
          "language":"javascript",
          "map":"function(doc) { if (doc.type) {emit(doc.type,1);}}",
          "reduce":"function(keys, values) { return sum(values);}"
        )
      );
    }

    return $this->_sources;
  }

  public function execute($url, $method = 'get', $data = '') {
    $result = false;
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

    if ($result !== false) {
      $result = json_decode($result, true);

      /*if (json_last_error() == JSON_ERROR_NONE) {
        $result = null;
      }*/
    }

    if ($this->logQueries && (count($this->_queriesLog) <= $this->_queriesLogMax) {
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


  public function create($model, $fields = array(), $values = array()) {
  }
  public function read($model, $queryData = array()) {
  }
  public function update($model, $fields = array(), $values = array()) {
  }
  public function delete($model, $id = null) {
  }
}