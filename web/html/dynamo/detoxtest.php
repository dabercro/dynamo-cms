<?php
$reqarr = array();
foreach ($_REQUEST as $key => $value)
  $reqarr[] = $key . '=' . $value;

header(sprintf("Location: %s://%s/web/detox/test?%s", $_SERVER["REQUEST_SCHEME"], $_SERVER["HTTP_HOST"], implode('&', $reqarr)));

exit;

?>
