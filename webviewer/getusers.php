<?php

$db = new SQLite3('match_data.db');

$query_string = "SELECT username FROM users ORDER BY username";

$stmt = $db->prepare($query_string);
$result = $stmt->execute();

$data = array();
while ($row = $result->fetchArray(SQLITE3_NUM)) {
    $data[] = $row;
}

echo json_encode($data);

?>
