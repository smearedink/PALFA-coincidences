<?php

$db = new SQLite3('match_data.db');

$ra_min = $_GET['ra_min'];
$ra_max = $_GET['ra_max'];
$dec_min = $_GET['dec_min'];
$dec_max = $_GET['dec_max'];

while ($ra_min < 0)
    $ra_min += 360;
while ($ra_max > 360)
    $ra_max -= 360;

if ($ra_min > $ra_max)
    $ra_connector = "OR";
else
    $ra_connector = "AND";    

$query_string = sprintf("SELECT name as psrname, ra_deg, dec_deg, period, dm FROM atnf WHERE ra_deg > %f %s ra_deg < %f AND dec_deg > %f AND dec_deg < %f", $ra_min, $ra_connector, $ra_max, $dec_min, $dec_max);

$stmt = $db->prepare($query_string);
$result = $stmt->execute();

$data = array();

$group_id = 'blank';
$counter = -1;
while ($row = $result->fetchArray(SQLITE3_ASSOC)) {
    $data[] = $row;
}

echo json_encode($data);

?>
