<?php
$dir = 'market_data';
$files = array_diff(scandir($dir), array('..', '.'));
echo json_encode(array_values($files));
?>