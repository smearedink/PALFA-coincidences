<?php

$db = new SQLite3('match_data.db');

$cand_id = $_GET['cand_id'];

$query_string = sprintf("SELECT group_id FROM cands WHERE cand_id = %s", $cand_id);
$stmt = $db->prepare($query_string);
$result = $stmt->execute();

if ($row = $result->fetchArray(SQLITE3_ASSOC))
    $group_id = $row['group_id'];

#$query_string = sprintf("SELECT c.group_id, c.cand_id, c.header_id, c.bary_period as period, c.dm, h.mjd, c.sigma, h.ra_deg, h.dec_deg FROM cands as c LEFT JOIN headers as h ON c.header_id=h.header_id INNER JOIN groups as g ON c.group_id=g.group_id WHERE c.group_id = \"%s\"", $group_id);
$query_string = sprintf("SELECT c.group_id, c.cand_id, c.header_id, c.db_version, c.bary_period as period, c.dm, h.mjd, c.sigma, h.ra_deg, h.dec_deg, c.match_prob FROM cands as c LEFT JOIN headers as h ON c.header_id=h.header_id AND c.db_version=h.db_version WHERE c.group_id = \"%s\"", $group_id);
$stmt = $db->prepare($query_string);
$result = $stmt->execute();

$data = array();

$group_id = 'blank';
$counter = -1;
while ($row = $result->fetchArray(SQLITE3_ASSOC)) {
    if ($row['group_id'] != $group_id) {
        $group_id = $row['group_id'];
        $counter++;
        $data[$counter] = array();
    }
    $data[$counter][] = $row;
}

echo json_encode($data);

?>
