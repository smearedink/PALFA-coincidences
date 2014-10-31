<?php

$db = new SQLite3('match_data.db');

$username = $_GET['username'];

$query_string = sprintf("INSERT INTO users(username) VALUES(\"%s\")", $username);
$stmt = $db->prepare($query_string);
$result = $stmt->execute();

$query_string = sprintf("ALTER TABLE groups ADD COLUMN \"%s\" INTEGER DEFAULT 0", $username);
$stmt = $db->prepare($query_string);
$result = $stmt->execute();

?>
