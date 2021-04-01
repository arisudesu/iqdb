<?php

# Connect to iqdb server, send it a command and parse the replies.
function request_srv($line) {
	global $iqdb_host, $iqdb_port, $iqdb_restart_cmd;

	for ($try = 0; $try < 3; $try++) {
		@$fp = fsockopen($iqdb_host, $iqdb_port, $errno, $errstr, 3);
		if ($fp) break;

		debug("Can't connect, errno=$errno errstr=$errstr try=$try"); flush();
		if ($errno == 111 /* ECONNREFUSED */ && $try == 2 && $iqdb_restart_cmd) {
			debug("Trying to restart iqdb: $iqdb_restart_cmd");
			exec($iqdb_restart_cmd, $out);
			debug("Output:\n".join("\n",$out),"\n");
		}
		sleep(2);
	}

	if (!$fp) 
		return array("fatal" => "Whoops, can't connect to database ;_;", "fatal2" => "Go to <a href='irc://irc.rizon.net/haruhidoujins'>#haruhidoujins@irc.rizon.net</a> to rant.");

	#debug("Sending: $line\n");
	fwrite($fp, "$line\n");
	fflush($fp);
	$res = array("info" => array(), "values" => array(), "results" => array());

	while ($line = fgets($fp, 1024)) {
		#debug("got $line");
		list($code,$rest) = split(" ", chop($line), 2);
		#debug("code=$code rest=$rest");
		switch($code) {
			case 100: array_push($res["info"], $rest);
				  break;
			case 101: list($var,$val) = split("=", $rest, 2);
				  $res["values"][$var] += $val;
				  break;
			case 200: array_push($res["results"], "-1 $rest");
				  break;
			case 201: array_push($res["results"], $rest);
				  break;
			case 300:
			case 301:
			case 302: $res["err"] = $rest;
				  break;
			default:  return array("fatal" => "Unsupported result code $code $rest");
		}
	}
	fclose($fp);

	return $res;
}

# For stats on how long the various search steps take.
function microtime_float() {
	list($usec, $sec) = explode(" ", microtime());
	return ((float)$usec + (float)$sec);
}

# Ask iqdb server for images most similar to given file.
function request_match($file, $querysrv, $options = array()) {
	global $services;

	$start = microtime_float();
	$res = array("results" => array());
	$numres = $options["numres"];
	if (!$numres) $numres = 16;
	$flags = $options["forcegray"] ? 2 : 0;
	if (is_array($querysrv)) {
		$res = "";
		$query = "";
		foreach ($querysrv as $db) {
			$srv=$services[$services[$db]];
			$rflags = $flags;
			if ($srv["uniqueset"]) $rflags |= 8;
			$db=$srv["db"];
			$res.="count $db\n";
			if ($query) $query.=' +';
			$query.=" $db $rflags $numres";
		}
		$query = $res."multi_query$query $file\ndone now\n";
		#debug("Doing multi query:\n$query");
		$res = request_srv($query);
		if ($res["results"]) $res["results"] = array_slice($res["results"], 0, $numres);
	} else {
		$srv=$services[$services[$querysrv]];
		$db = $srv["db"];
		if ($srv["uniqueset"]) $flags |= 8;
		$query = "count $db\nquery $db $flags $numres $file\ndone now\n";
		#debug("Doing simple query for $querysrv:\n$query");
		$res = request_srv($query);
	}
	if ($res["fatal"]) return $res;
	$time = microtime_float() - $start;
	$start = microtime_float();
	$result = array("values" => $res["values"], "match" => array());

	foreach ($res["results"] as $line) {
		list($dbid,$id,$sim,$width,$height) = split(" ", $line);
		if (!$width) $width=-1;
		if (!$height) $height=-1;
		if ($dbid == -1) $dbid = $services[$services[$querysrv]]["db"];
		#debug("Line: $line  DB: $dbid -> ".$services[$dbid]." ID/sim/w/h $id $sim $width $height");

		$srv = $services[$services[$dbid]];
		if ($srv["process"]) {
			$arg = array("dbid" => $dbid, "id" => $id, "sim" => $sim, "width" => $width, "height" => $height, "file" => $file);
			$match = $srv["process"]($arg);
			if ($match["err"]) return array('err' => $match["err"]);
			array_push($result["match"], $match);
			continue;
		}

		$srvdir = $srv["fullpath"];
		$pref = substr($id,0,1)."/".substr($id,1,1)."/".substr($id,2,1);
		#debug("$id -> $srvdir/$pref");
		$files = glob("$srvdir/$pref/$id*");
		if (count($files) > 1) continue;
		if (count($files) == 0) $files = glob("$srvdir/xx/$id-*");
		#debug("result: $line. ID:$id Pref:$pref sim:$sim files:".join(";",$files));
		if (count($files) != 1) continue;
		if (!ereg("/(".$srv["dir"]."/[0-9a-fx\/]+/([0-9a-f-]+)\.jpg)$", $files[0], $eres)) continue;
		array_push($result["match"], array('sim' => $sim, 'md5' => substr($eres[2],-32), 'fname' => $eres[1], 'tag' => $srv["dir"], 'width' => $width, 'height' => $height));
	}
	$dbtime = microtime_float() - $start;
	#debug("Got ".count($res["results"])." results parsed into ".count($result["match"])." matches.");
	if (count($res["results"]) && !count($result["match"])) return array('err' => "Thumbnails not found.");
	if (!count($result["match"])) return array('err' => "Database error: ".($res["err"]?$res["err"]:"No matches returned."));
	$result["stats"] = sprintf("query %.3fs db %.3fs", $time, $dbtime);
	return $result;
}

# Process a result set: find if there is a relevant match and what the threshold is.
function process_match($match, $ignoremd5 = false, $ignoretag = '') {
	if(!is_array($match)) return array("err" => "Database returned nothing.");
	$sum = 0;
	$count = 0;
	$sqsum = 0;
	foreach ($match as $page) {
		if ($ignoremd5 && $page["md5"] === $ignoremd5 && $page["tag"] === $ignoretag) continue;
		$sum += $page['sim'];
		$sqsum += $page['sim'] * $page['sim'];
		$count++;
	}
	if(!$count) return array("err" => "Database returned nothing.");
	$avg = $sum/$count;
	$stddev = sqrt($sqsum/$count - $avg*$avg);
	$minrel = $avg + $stddev/2;
	$thres = array("avg" => $avg, "stddev" => $stddev, "numrel" => 0);

	if ($stddev < 5 && $match[0]['sim'] < 95) {
		$thres["minsim"] = $minrel;
		$minrel = 90;
	} else {
		foreach ($match as $page) {
			#debug("Checking sim=".$page['sim'].", sum=$sum sqsum=$sqsum avg=$avg stddev=$stddev minrel=$minrel count=$count");
			if ($page['sim'] < $minrel) break;
			$thres["numrel"]++;
			if ($ignoremd5 && $page["md5"] === $ignoremd5 && $page["tag"] === $ignoretag) continue;
			if (!--$count) break;
			$sum -= $page['sim'];
			$sqsum -= $page['sim'] * $page['sim'];
			$avg = $sum/$count;
			$stddev = sqrt($sqsum/$count - $avg*$avg);
			if ($stddev < 5) continue;
			$minrel = $avg + $stddev/2;
		}
	}
	#debug("avg=".$thres["avg"]." stddev=".$thres["stddev"]." minrel=$minrel numrel=".$thres["numrel"]);
	$thres["minrel"] = $minrel;
	return $thres;
}

# Save JPEG thumbnail of $org in $img. Return thumb info or error.
function make_thumb($img, $org) {
	global $thu_dir, $thudim, $maxdim;

	@$size = getimagesize($img);
	if (!$size) 
		return array("err" => "NotImage", "errarg" => array(htmlentities($org)));

	$thumb = array("org" => $org, "size_x" => $size[0], "size_y" => $size[1]);
	#debug("size ".join(":", $size));
	if ($size[0] > $maxdim || $size[1] > $maxdim)
		return array("err" => "TooBig", "errarg" => array(htmlentities($org), $size[0], $size[1]), "erralt" => "ResizeIt");

	if ($size[2] == IMAGETYPE_GIF)
		@$image = imagecreatefromgif($img);
	else if ($size[2] == IMAGETYPE_JPEG)
		@$image = imagecreatefromjpeg($img);
	else if ($size[2] == IMAGETYPE_PNG)
		@$image = imagecreatefrompng($img);
	else
		return array("err" => "NotSupp", "errarg" => array(htmlentities($org), image_type_to_mime_type($size[2])));

	if (!$image)
		return array("err" => "NotImage", "errarg" => array(htmlentities($org)));

	for ($count = 0; $count < 32; $count++) {
		$seed = substr(md5($seed.microtime().posix_getpid()), 0, 8);
		$thumb["seed"] = $seed;
		$thumb["fname"] = "thu_$seed.jpg";
		$thumb["name"] = "$thu_dir/".$thumb["fname"];
		@$fh = fopen($thumb["name"], "x");
		#debug(array("thu $count: $thuname", $fh ? "success" : "failed"));
		if ($fh) break;
	};
	if (!$fh)
		return array("err" => "ThumbErr");
	fclose($fh);

	/* Create thumbnail in PHP */
	if ($size[0] > $size[1]) {
		$thu_x = $thudim;
		$thu_y = floor($size[1] * $thudim / $size[0]);
	} else {
		$thu_y = $thudim;
		$thu_x = floor($size[0] * $thudim / $size[1]);
	}
	$thumb["thu_x"] = $thu_x;
	$thumb["thu_y"] = $thu_y;
	#debug("thumbnail $thuname: $thu_x $thu_y from $size[0] $size[1]");
	$start = microtime_float();
	$thu = @imagecreatetruecolor($thu_x, $thu_y);
	if (!$thu) {
		@imagedestroy($image);
		return array("err" => "ThumbErr");
	}
	if (!@imagecopyresampled($thu, $image, 0, 0, 0, 0, $thu_x, $thu_y, $size[0], $size[1])) {
		@imagedestroy($image);
		@imagedestroy($thu);
		return array("err" => "ThumbErr");
	}
	imagedestroy($image);
	if (!@imagejpeg($thu, $thumb["name"], 98)) {
		@imagedestroy($thu);
		return array("err" => "ThumbErr");
	}
	imagedestroy($thu);
	$end = microtime_float();
	$thumb["stats"] = sprintf(" thu %.3fs", $end-$start);
	return $thumb;
}

# Output list of matches as XML.
function produce_xml($match, $thumb, $thres) {
	global $base_url, $services;

	$thumburl="thu/".$thumb["fname"];
	if ($thumb["cname"]) $thumburl=$thumb['cname'];
	echo "<matches preview='$base_url$thumburl' threshold='".$thres["minrel"]."'>\n";
	foreach ($match as $page) {
		if ($page['md5']) $m = '" md5="'.$page['md5'].'" width="'.$page['width'].'" height="'.$page['height'].'"';
		else $m = '" id="'.$page['id'].'" name="'.htmlentities($page['eng']).'"';
		if ($page['fname']) $m .= " preview='$base_url".htmlentities($page['fname'])."'";
		echo '<match sim="'.$page['sim'].'" service="'.$services[$page['tag']]["site"]."$m/>\n";
	}
	echo "</matches>";
}

?>
