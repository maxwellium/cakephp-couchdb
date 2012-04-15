<?php

class CouchDBAppModel extends AppModel {
  public $useDbConfig = 'couchDB';
  public $database = null;

  public $primaryKey = '_id';
  public $revisionKey = '_rev';
}