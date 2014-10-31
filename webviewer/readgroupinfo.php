<?php

$db = new SQLite3('match_data.db');

$group_id = $_GET['group_id'];
#$group_id = str_replace("%2B", "+", $group_id);
$query_string = 'SELECT * FROM groups WHERE group_id=' . $group_id;
$query_string = str_replace("\\", "", $query_string);


#$stmt = $db->prepare($query_string);
#$stmt = $db->prepare('SELECT * FROM groups WHERE group_id=:id');
#$stmt->bindParam(':id', $group_id, SQLITE3_TEXT);

#$result = $stmt->execute();

$result = $db->query($query_string);

if ($row = $result->fetchArray(SQLITE3_ASSOC))
    echo json_encode($row);

?>
