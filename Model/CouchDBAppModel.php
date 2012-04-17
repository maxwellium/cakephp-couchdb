<?php

class CouchDBAppModel extends AppModel {
  public $useDbConfig = 'couchDB';
  public $database = null;

  public $primaryKey = '_id';
  public $revisionKey = '_rev';

  /*
   * had to overwrite these two functions since we will not predefine our schema :)
   * nonsqlfreedom
   */
  public function schema($field = false) {
    if (isset($this->data[$this->alias]) && is_array($this->data[$this->alias])) {
      $this->_schema = array_flip(array_keys($this->data[$this->alias]));
    } else {
      $this->_schema = array();
    }

    if (is_string($field)) {
      if (isset($this->_schema[$field])) {
        return $this->_schema[$field];
      } else {
        return null;
      }
    }
    return $this->_schema;
  }
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

    // this is the change, rebuilding schema from data everytime:

    //if (empty($this->_schema)) {
    $this->schema();
    //}

    if ($this->_schema != null) {
      return isset($this->_schema[$name]);
    }
    return false;
  }


  // simplify counting
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
          } else {
            return intval($results[0][$key]['count']);
          }
        }
      }
      return false;
    }
  }
}