<?php
/**
 * CouchDBAppModel
 * 
 * Provides the CouchDB plugin base model to be extended
 * 
 * @author maxwellium - https://github.com/maxwellium
 * @package CouchDB
 * @subpackage Model
 * @filesource
 */
class CouchDBAppModel extends AppModel {

/**
 * The db config to be used
 * 
 * @var string
 * @access public
 */
	public $useDbConfig = 'couchDB';

/**
 * The database to be used
 * 
 * @var string
 * @access public
 */
	public $database = null;

/**
 * The current revision of the couch document
 * 
 * @var string
 * @access public
 */
	public $_rev = null;

/**
 * The Model's primary key
 * 
 * @var string
 * @access public
 */
	public $primaryKey = '_id';

/**
 * The revision key
 * 
 * @var string
 * @access public
 */
	public $revisionKey = '_rev';

/**
 * (non-PHPdoc)
 * @see Model::schema()
 */
	public function schema($field = false) {
		$this->_schema = array_flip(array_keys($this->validate));

		if (isset($this->data[$this->alias]) && is_array($this->data[$this->alias])) {
			$this->_schema = array_merge(
				$this->_schema,
				array_flip(array_keys($this->data[$this->alias])));
		}

		if (is_string($field)) {
			if (isset($this->_schema[$field])) {
				return $this->_schema[$field];
			}
			return null;
    	}

		return $this->_schema;
	}

/**
 * (non-PHPdoc)
 * @see Model::hasField()
 */
	public function hasField($name, $checkVirtual = false) {
		if (is_array($name)) {
			foreach ($name as $n) {
				if ($this->hasField($n, $checkVirtual)) {
					return $n;
				}
			}
			return false;
		}

		if ($checkVirtual && !empty($this->virtualFields)) {
			if ($this->isVirtualField($name)) {
				return true;
			}
		}

		// this is the change: rebuilding schema from data everytime so all fields are submitted
		$this->schema();

		if ($this->_schema != null) {
			return isset($this->_schema[$name]);
		}

		return false;
	}

/**
 * (non-PHPdoc)
 * @see Model::_findCount()
 */
	protected function _findCount($state, $query, $results = array()) {
		if ($state === 'before') {
			$query['order'] = false;
			$query['fields'] = 'count';
			return $query;
		} elseif ($state === 'after') {
			foreach (array(0, $this->alias) as $key) {
				if (isset($results[0][$key]['count'])) {
					if (($count = count($results)) > 1) {
						return $count;
					}
					return intval($results[0][$key]['count']);
				}
			}
			return false;
		}
	}

/**
 * Updates the revision to the latest one
 * 
 * @return void
 * @access public
 * 
 */
	public function updateRevision() {
		if (!is_array($this->data)) {
			$this->data = array();
		}
		$this->data[$this->revisionKey] = $this->getDataSource()->getRevision($this);
	}

}
