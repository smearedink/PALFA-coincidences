<?php

$db = new SQLite3('match_data.db');

$group_id = $_GET['group_id'];
$username = $_GET['username'];
$new_rating = $_GET['new_rating'];
$query_string = "UPDATE groups SET \"" . $username . "\"=" . $new_rating . " WHERE group_id=" . $group_id;

#$stmt = $db->prepare($query_string);
#$result = $stmt->execute();
$db->query($query_string);

?>
