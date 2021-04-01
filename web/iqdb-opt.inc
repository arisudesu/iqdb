<? # vim: syntax=php

# Settings for iqdb-xml.php
$iqdb_host = "localhost";		# Host and
$iqdb_port = 5566;			# port where iqdb server is listening.
$base_dir = "/opt/iqdb/foo/";		# Directory containing image thumbnails.
$thu_dir = "$base_dir/thu/";		# Directory where generated thumbs are to be saved.
$base_url = "http://localhost/";	# Base URL corresponding to $base_dir.
$service = "localhost";			# Service name in XML reply.
$maxdim = 2500;				# Maximum allowed image dimensions for thumbnailing.
$thudim = 150;				# Thumbnail dimensions.

# Array of database infos for the iqdb server.
# One set of values for each DB.
$services = array(
	0 => "localhost",		# DBID -> service name
	"-local" => "localhost",	# tag -> service name
	"localhost" => array(		# service name ->
		"site" => "localhost",		# service host (also serves as service name for XML output)
		"dir" => "thumbs",		# thumbnail dir (indexed)
		"db" => 0,			# DBID
		#"process" => "my_process",	# optional: function to resolve matched image IDs
		# may hold other keys as required by the front-end.
	),
);

?>
