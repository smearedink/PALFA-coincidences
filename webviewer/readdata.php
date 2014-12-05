<?php

$db = new SQLite3('match_data.db');

$p_min = $_GET['p_min'];
$p_max = $_GET['p_max'];
$sigma_min = $_GET['sigma_min'];
$sigma_max = $_GET['sigma_max'];
$ncands_min = $_GET['ncands_min'];
$ncands_max = $_GET['ncands_max'];
$time_span_min = $_GET['time_span_min'];
$time_span_max = $_GET['time_span_max'];
$check0 = $_GET['check0'];
$check1 = $_GET['check1'];
$check2 = $_GET['check2'];
$check3 = $_GET['check3'];
$check4 = $_GET['check4'];
$group_id = $_GET['group_id'];

$do_full_query = true;

if (!empty ($group_id)) {
    $do_full_query = false;

    #$query_string = sprintf("SELECT c.group_id, c.cand_id, c.header_id, c.bary_period as period, c.dm, h.mjd, c.sigma, h.ra_deg, h.dec_deg FROM cands as c LEFT JOIN headers as h ON c.header_id=h.header_id INNER JOIN groups as g ON c.group_id=g.group_id WHERE c.group_id = %s", $group_id);
    $query_string = sprintf("SELECT c.group_id, c.cand_id, c.header_id, c.db_version, c.bary_period as period, c.dm, h.mjd, c.sigma, h.ra_deg, h.dec_deg FROM cands as c LEFT JOIN headers as h ON c.header_id=h.header_id AND c.db_version=h.db_version WHERE c.group_id = %s", $group_id);
    
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
    
    if (count($data))
        echo json_encode($data);
    else
        $do_full_query = true;
}

if ($do_full_query) {
    $query_string = "SELECT c.group_id, c.cand_id, c.header_id, c.db_version, c.bary_period as period, c.dm, h.mjd, c.sigma, h.ra_deg, h.dec_deg FROM cands as c LEFT JOIN headers as h ON c.header_id=h.header_id AND c.db_version=h.db_version INNER JOIN groups as g ON c.group_id=g.group_id WHERE g.ncands > 0";

    //$cond_count = 0;
    if (!empty($p_min)) $query_string .= sprintf(" AND g.min_period >= %f", $p_min);
    if (!empty($p_max)) $query_string .= sprintf(" AND g.min_period <= %f", $p_max);
    if (!empty($sigma_min)) $query_string .= sprintf(" AND g.max_sigma >= %f", $sigma_min);
    if (!empty($sigma_max)) $query_string .= sprintf(" AND g.max_sigma <= %f", $sigma_max);
    if (!empty($ncands_min)) $query_string .= sprintf(" AND g.ncands >= %d", $ncands_min);
    if (!empty($ncands_max)) $query_string .= sprintf(" AND g.ncands <= %d", $ncands_max);
    if (!empty($time_span_min)) $query_string .= sprintf(" AND g.time_span >= %f", $time_span_min);
    if (!empty($time_span_max)) $query_string .= sprintf(" AND g.time_span <= %f", $time_span_max);
    $ncheck = 0;
    if (!empty($check0)) {
        if ($ncheck > 0) $query_string .= sprintf(" OR \"%s\" = 0", $check0);
        else $query_string .= sprintf(" AND (\"%s\" = 0", $check0);
        $ncheck++;
    }
    if (!empty($check1)) {
        if ($ncheck > 0) $query_string .= sprintf(" OR \"%s\" = 1", $check1);
        else $query_string .= sprintf(" AND (\"%s\" = 1", $check1);
        $ncheck++;
    }
    if (!empty($check2)) {
        if ($ncheck > 0) $query_string .= sprintf(" OR \"%s\" = 2", $check2);
        else $query_string .= sprintf(" AND (\"%s\" = 2", $check2);
        $ncheck++;
    }
    if (!empty($check3)) {
        if ($ncheck > 0) $query_string .= sprintf(" OR \"%s\" = 3", $check3);
        else $query_string .= sprintf(" AND (\"%s\" = 3", $check3);
        $ncheck++;
    }
    if (!empty($check4)) {
        if ($ncheck > 0) $query_string .= sprintf(" OR \"%s\" = 4", $check4);
        else $query_string .= sprintf(" AND (\"%s\" = 4", $check4);
        $ncheck++;
    }
    if ($ncheck > 0) $query_string .= ")";

    $query_string .= " ORDER BY g.min_period";
    
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
    
    if (count($data))
        echo json_encode($data);
}

?>
