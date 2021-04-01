<?php

$iqdb_host = "localhost";	# Host and
$iqdb_port = 5566;		# port where iqdb server is listening.
$base_dir = "/opt/iqdb/foo/";	# Directory containing image thumbnails.

# Connect to iqdb server, send it a command and parse the replies.
function request_srv($line, $err = 0) {
	global $iqdb_host,$iqdb_port;

	for ($try = 0; $try < 5; $try++) {
		@$fp = fsockopen($iqdb_host, $iqdb_port, $errno, $errstr, 30);
		if ($fp) break;

		sleep(2);
	}

	if (!$fp) {
		die("Can't connect to database.");
	}

	fwrite($fp, "$line\n");
	fflush($fp);
	$res = array("info" => array(), "values" => array(), "results" => array());

	while ($line = fgets($fp, 1024)) {
		list($code,$rest) = split(" ", chop($line), 2);
		switch($code) {
			case 100: array_push($res["info"], $rest); break;
			case 101: list($var,$val) = split("=", $rest, 2); $res["values"][$var] += $val; break;
			case 200: array_push($res["results"], "-1 $rest"); break;
			case 201: array_push($res["results"], $rest); break;
			case 300:
			case 301:
			case 302: $res["err"] = $rest; if ($err) die("Error: $rest"); break;
			default: die("Unsupported result code $code $rest");
		}
	}
	fclose($fp);

	return $res;
}

# Ask iqdb server for images most similar to given file.
function request_match($file) {
	global $base_dir;

	$res = request_srv("count 0\nquery 0 0 16 $file\ndone now\n");
	$result = array("values" => $res["values"], "match" => array());

	foreach ($res["results"] as $line) {
		list($dbid,$id,$sim,$width,$height) = split(" ", $line);

		# Resolve imgid => md5
		$pref = substr($id,0,1)."/".substr($id,1,1)."/".substr($id,2,1);
		$files = glob("$base_dir/$pref/$id*");
		if (count($files) > 1) continue;
		if (count($files) == 0) $files = glob("$base_dir/xx/$id-*");
		if (count($files) > 1) continue;
		if (!ereg("/([0-9a-fx\/]+/([0-9a-f-]+)\.jpg)$", $files[0], $eres)) continue;
		array_push($result["match"], array('sim' => $sim, 'md5' => substr($eres[2],-32),
						'fname' => $eres[1], 'width' => $width, 'height' => $height));
	}
	if (!count($result["match"])) return array('err' => $res["err"]);
	return $result;
}

$res = request_match("test.jpg");
var_dump($res);

?>
