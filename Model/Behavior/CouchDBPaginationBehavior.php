<?php
class CouchDBPaginationBehavior extends ModelBehavior {

  public $settings;

  public function setup(Model $model, $config = array()) {
    if (!isset($this->settings[$model->alias])) {
      $this->settings[$model->alias] = array();
    }
    $this->settings[$model->alias] = array_merge(
      $this->settings[$model->alias], (array)$config
    );
  }

  public function paginate(Model $model, $conditions, $fields, $order, $limit, $page = 1, $recursive = null, $extra = array()) {
    $recursive = -1;

    $params = array(
      'include_docs'=>false,
      'limit' => $limit,
      'skip' => $limit * ($page -1)
    );

    if (is_array($order) && (count($order) == 1)) {
      $view = array_keys($order);
      if ($order[$view[0]] == 'desc') {
        $params['descending'] = true;
      }

      $view = explode('.',$view[0]);
      $view = 'by' . ucfirst($view[1]);
    } else {
      $view = 'by' . ucfirst($model->displayField);
    }

    $body = $model->find(
      'all',
      array(
        'design' => strtolower($model->name),
        'view' => $view,
        'params' => $params
      )
    );

    $result = array();
    if (isset($body['rows'])){
      foreach($body['rows'] as $row) {
        $result[] = array($model->alias => $row['value']);
      }
    }

    return $result;
  }

}