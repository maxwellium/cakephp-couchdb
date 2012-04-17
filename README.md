CouchDB DataSource for CakePHP
==============================

This plugin aims at cakephp-developers who wish to easily embed CouchDB into their cakephp projects.
Since it's not yet fully done, feel free to contribute or comment. Basic CakePhp model functions that make developing with cake so comfortable will be implemented in this datasource.

Getting started
---------------

pull the plugin into your app/Plugin folder


Have a db-config somewhat like this:

    class DATABASE_CONFIG {
      .
      .
      .
      public $couchdb = array(
        'host'      => 'localhost', // optional
        'port'      => 5984,        // optional
        'login'     => 'root',
        'password'  => 'root',
        'models'    => 'type',      // (optional)
        'database'  => 'cake',      // can be overridden in Model->database
      );
    }


$couchdb['models'] hereby refers to the document field, that most nosql-developers use to distinguish between models.
check out http://guide.couchdb.org/draft/validation.html#type on this matter


Load Plugin at the end of bootstrap like this

    CakePlugin::load('CouchDB');


in your application you can then create models like this:

    <?php
      App::uses('CouchDBAppModel', 'CouchDB.Model');
      class User extends CouchDBAppModel {
        .
        .
        .
      }
    ?>

Usage
-----

### Create
### Read
### Update

If no _rev is set in data, it will be fetched. Otherwise current revision will be checked against possible change and function returns false if separate change has occured in between.

### Delete