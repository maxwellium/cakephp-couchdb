<?php
/**
 * CouchDBSource Datasource file
 *
 * Provides a CouchDB Datasource for CakePHP
 *
 * @author maxwellium - https://github.com/maxwellium
 * @package CouchDB
 * @subpackage Model.Datassource
 * @filesource
 */
App::uses('HttpSocket', 'Network/Http');

/**
 * This is the CouchDB Datasource class
 *
 * @package CouchDB
 * @subpackage Model.Datasource
 */
class CouchDBSource extends DataSource {

/**
 * The Datasource's default configuration
 *
 * @var array
 * @access protected
 */
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

/**
 * Wether or not queries should be logged
 *
 * @var boolean
 * @access public
 */
	public $logQueries = true;

/**
 * Holds the various query logs for a request
 *
 * @var array
 * @access protected
 */
	protected $_queriesLog = array();

/**
 * Holds the query count
 *
 * @var integer
 * @access protected
 */
	protected $_queriesCnt = 0;

/**
 * Holds the queries time
 *
 * @var integer
 * @access protected
 */
	protected $_queriesTime = null;

/**
 * The maximum amount of queries that may be logged
 *
 * @var integer
 * @access protected
 */
	protected $_queriesLogMax = 200;

/**
 * CouchDBSource Constructor
 *
 * @param array $config The configuration for the Datasource
 * @param boolean $autoConnect If true, automatically connects the Datasource
 *
 * @return void
 * @access public
 * @link http://api.cakephp.org/class/data-source#method-DataSource__construct
 */
	public function __construct($config = array(), $autoConnect = true){
		parent::__construct($config);
		if ($autoConnect) {
			$this->connect();
		}
	}

/**
 * (non-PHPdoc)
 * @see DataSource::setConfig()
 */
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
		$this->config = array_replace_recursive($this->_baseconfig, $this->config, $config, $translatedConfig);
	}

/**
 * Connects the Datasource
 *
 * @return boolean
 * @access public
 * @throws MissingConnectionException
 */
	public function connect() {
		if ($this->connected !== true) {
			try {
				$this->Socket = new HttpSocket($this->config);

				switch ($this->config['authType']) {
					case 'cookie':
						$this->connected = $this->_cookieAuth();
						break;
					case 'oauth':
						$this->connected = false;
						break;
					case 'none':
						$this->connected = true;
						break;
					case 'basic':
					default:
						$this->connected = $this->_basicAuth();
						break;
				}
			} catch (Exception $e) {
				throw new MissingConnectionException(array('class' => $e->getMessage()));
			}
		}
		return $this->connected;
	}

/**
 * Implements Cookie authentication
 *
 * @return boolean
 * @access protected
 * @throws CakeException
 */
	protected function _cookieAuth() {
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
		$errors = $this->_getErrors($response, $result);

		if ($this->_isError($errors)) {
			throw new CakeException($this->_errorString($errors));
		}

		return true;
	}

/**
 * Implements Basic Authentication
 *
 * @return boolean
 * @access protected
 *
 * @throws CakeException
 *
 * @see http://wiki.apache.org/couchdb/HttpGetRoot
 */
	protected function _basicAuth() {
		$this->Socket->config['request']['uri']['user'] = $this->config['login'];
		$this->Socket->config['request']['uri']['pass'] = $this->config['password'];

		$response = $this->Socket->get('/');
		$result = json_decode($response->body(), true);

		$errors = $this->_getErrors($response, $result);

		if ($this->_isError($errors)) {
			throw new CakeException($this->_errorString($errors));
		}
		return true;
	}

/**
 * Reconnects the Datasource
 *
 * @param array $config The config to be used for the reconnect
 *
 * @return boolean
 * @access public
 *
 * @throws MissingConnectionException
 */
	public function reconnect($config = array()) {
		$this->disconnect();
		$this->setConfig($config);
		$this->_sources = array();
		return $this->connect();
	}

/**
 * Disconnects the datasource
 *
 * @return boolean
 * @access public
 */
	public function disconnect() {
		if (isset($this->results) && is_resource($this->results)) {
			$this->results = null;
			$this->Socket->reset();
		}
		$this->connected = false;
		return !$this->connected;
	}

/**
 * (non-PHPdoc)
 * @see DataSource::close()
 */
	public function close() {
		$this->disconnect();
	}

/**
 * Performs a query on the couchDB database
 *
 * @param string $url The URL to be queried
 * @param string $method The HTTP method to be used (GET, POST, PUT, DELETE, HEAD)
 * @param array $query The query params
 * @param array $data The data to be passed
 *
 * @return array The result array consisting of 'body', 'errors' and 'headers' keys
 * @access public
 *
 * @throws CakeException
 *
 * @todo remove switch, validate method against an array and use request on all
 */
	public function query($url, $method = 'get', $query = array(), $data = array()) {
		$t = microtime(true);
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
			// http://wiki.apache.org/couchdb/HTTP_Document_API#HEAD
			// to fetch revision
			case 'head':
				$response = $this->Socket->request(array('method' => 'HEAD', 'uri' => $url, 'body' => $data));
				break;
			case 'get':
			default:
				$response = $this->Socket->get($url, $query, $data);
				break;
		}

		$t = round((microtime(true) - $t) * 1000, 0);
		$result = json_decode($response->body(), true);
		$errors = $this->_getErrors($response, $result);

		if ($this->logQueries && (count($this->_queriesLog) <= $this->_queriesLogMax)) {
			$this->_queriesCnt++;
			$this->_queriesTime += $t;
			$this->_queriesLog[] = array(
				'query'   => $method . ' ' . $url . ' ' . print_r($query, true) . ' ' . print_r($data, true),
				'params'  => $data,
				'affected'=> 0,
				'numRows' => 0,
				'took'    => $t,
				'error'   => $this->_errorString($errors)
			);
		}

		// the reason we're only throwing json-errors, is that couchDB can of course respond
		// with a 404 when we're trying to find a document. since cakephp prefetches records
		// when an id is set, this would destroy update logic.
		if (($errors['json'] !== false) || ($errors['couch']['error'] == 'unauthorized') ||
				(isset($errors['couch']['reason']) && ($errors['couch']['reason']) == 'no_db_file')) {

			throw new CakeException($this->_errorString($errors));
		}

		return array('body' => $result, 'errors' => $errors, 'headers' => $response->headers);
	}

/**
 * Gets the errors from the HttpResponse object
 *
 * @param HttpResponse &$response A reference to the HttpResponse object
 * @param stdClass &$result A reference to the stdClass result object
 *
 * @return array The errors array
 * @access protected
 *
 * @see http://guide.couchdb.org/draft/api.html on this test
 */
	protected function _getErrors(&$response, &$result) {
		$errors = array(
			'http'    => false,
			'couch' => false,
			'json'    => false
		);

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

/**
 * Creates an error string from the errors array
 *
 * @param array $errors The errors array
 *
 * @return string The error message
 * @access protected
 */
	protected function _errorString($errors) {
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

/**
 * Checks if there was an error during the request
 *
 * @param array $errors The errors array
 *
 * @return boolean
 * @access protected
 */
	protected function _isError($errors) {
		foreach ($errors as $error) {
			if ($error !== false) {
				return true;
			}
		}
		return false;
	}

/**
 * Gets the queries log
 *
 * @param boolean $sorted Should the log entries be sorted
 * @param boolean $clear Should the log be cleared after fetching
 *
 * @return array The log result Array
 * @access public
 */
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

/**
 * Gets the escaped DB string
 *
 * @param string $modelDB [optional] The db to use, defaults to the configured database
 *
 * @return string The escaped db name
 * @access public
 *
 * @see http://wiki.apache.org/couchdb/HTTP_database_API
 */
	public function getDB($modelDB = null) {
		// ? it doesn't tell whether $()+- need to be escaped as well..?
		return str_replace('/', '%2F',
			is_null($modelDB) ? $this->config['database'] : $modelDB);
	}

/**
 * (non-PHPdoc)
 * @see DataSource::create()
 * @see http://wiki.apache.org/couchdb/HTTP_Document_API#POST
 * @see http://wiki.apache.org/couchdb/HTTP_Document_API#PUT
 */
	public function create(Model $model, $fields = null, $values = null) {
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

		if ($this->config['models'] !== false && !isset($data[$this->config['models']])) {
			$data[$this->config['models']] = strtolower($model->name);
		}

		$url = '/'. $this->getDB($model->database) . '/' . $id;
		$response = $this->query($url, 'put', array(), $data);

		if (!isset($response['body']['ok']) || $response['body']['ok'] !== true) {
			$model->onError();
			return false;
		}

		$model->data = $data;
		$model->id = $response['body']['id'];
		$model->data[$model->primaryKey] = $response['body']['id'];
		$model->data[$model->revisionKey] = $response['body']['rev'];

		return $data;
	}

/**
 * (non-PHPdoc)
 * @see DataSource::read()
 * @see http://stackoverflow.com/questions/3924089/previous-link-equivalent-of-limit-x-offset-y
 * @see http://guide.couchdb.org/draft/recipes.html#pagination
 */
	public function read(Model $model, $queryData = array(), $recursive = null) {
		$url = '/' . $this->getDB($model->database) . '/';
		$params = array();

		if (isset($queryData['view']) && isset($queryData['design'])) {
			$url .= '_design/' . $queryData['design'] . '/_view/' . $queryData['view'];
		} elseif(isset($queryData['conditions'][$model->alias . '.' . $model->primaryKey])) {
			$url .= $queryData['conditions'][$model->alias . '.' . $model->primaryKey];
		} else {
			$url .= '_all_docs';
			if (!empty($queryData['limit'])) {
				$params['limit'] = $queryData['limit'];
				$params['descending'] = 'true';

				if (!empty($queryData['offset'])) {
					$params['startkey'] = json_encode($queryData['offset']);
					$params['skip'] = 1;
          			// FIXME: ? increase skip value for real pagination with page jumping? page * 10 e.g.?
				}
			}
		}

		if ($queryData['fields'] == 'count') {
			if (isset($queryData['params']['limit'])) {
				unset($queryData['params']['limit']);
			}
		}

		if (isset($queryData['params']) && is_array($queryData['params'])) {
			foreach ($queryData['params'] as $parameter => $value) {
				$params[$parameter] = json_encode($value);
			}
		}

		$response = $this->query($url, 'get', $params);

		if ($this->_isError($response['errors'])) {
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
			if ($queryData['fields'] == 'count') {
				// document count is requested
				$result[] = array(
					$model->alias => array(
						'count' => $response['body']['total_rows']
					)
				);
			} else {
				// a collection of documents is requested
				if ( isset($params['include_docs']) && ($params['include_docs'] == 'true') ) {
					$value = 'doc';
				} else {
					$value = 'value';
				}
				if (isset($response['body']['rows']) && !empty($response['body']['rows'])){
					foreach($response['body']['rows'] as $row) {
						$result[] = array($model->alias => $row[$value]);
					}
				}
			}
		}
		return $result;
	}

/**
 * (non-PHPdoc)
 * @see DataSource::update()
 */
  public function update(Model $model, $fields = null, $values = null, $conditions = array()) {
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

			if ($this->_isError($response['errors'])) {
				return false;
			} else {
				$data = array_merge($response['body'], $data);
				$data[$model->revisionKey] = $response['body'][$model->revisionKey];
			}
		}

		if ($this->config['models'] !== false && !isset($data[$this->config['models']])) {
			$data[$this->config['models']] = strtolower($model->name);
		}

		$response = $this->query($url, 'put', array(), $data);

		if (!isset($response['body']['ok']) || $response['body']['ok'] !== true) {
			$model->onError();
			return false;
		}

		if (($this->config['models'] !== false) && isset($data[$this->config['models']])) {
			unset($data[$this->config['models']]);
		}

		$model->data = $data;
		$model->id = $response['body']['id'];
		$model->data[$model->primaryKey] = $response['body']['id'];
		$model->data[$model->revisionKey] = $response['body']['rev'];

		return true;
	}

/**
 * (non-PHPdoc)
 * @see DataSource::delete()
 */
	public function delete(Model $model, $conditions = array()) {
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
		} else {
			$revision = $this->getRevision($model, $id);
		}

		if (($id === false) || ($revision === false)) {
			return false;
		}

		$url = '/'. $this->getDB($model->database) . '/' . $id . '?rev=' . $revision;
		$response = $this->query($url, 'delete');

		return !$this->_isError($response['errors']) && ($response['body']['ok'] === true);
	}

/**
 * Gets the revision for a couch document for Model $model
 *
 * @param Model $model Model object
 * @param string $id the UUID
 *
 * @return string The current revision
 * @access public
 *
 * @see http://wiki.apache.org/couchdb/HTTP_Document_API#HEAD
 */
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
		$response = $this->query($url, 'head');

		if ($this->_isError($response['errors']) || !isset($response['headers']['Etag'])) {
			return false;
		}

		return trim($response['headers']['Etag'], '"');
	}

}
