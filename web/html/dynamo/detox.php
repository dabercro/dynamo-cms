<?php
$reqarr = array();
foreach ($_REQUEST as $key => $value)
  $reqarr[] = $key . '=' . $value;

if (count($reqarr) != 0) {
  $request = '?' . implode('&', $reqarr)));

header(sprintf("Location: %s://%s/web/detox%s", $_SERVER["REQUEST_SCHEME"], $_SERVER["HTTP_HOST"], $request));

exit;

?>
