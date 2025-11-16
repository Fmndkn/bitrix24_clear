<?php
error_reporting(E_ALL & ~E_NOTICE);
header("Content-type: text/html; charset=utf-8");

$lang = '';

if (isset($_REQUEST['lang']))
{
	$lang = $_REQUEST['lang'];

	if (!in_array($lang, ['ru', 'en']))
	{
		$lang = 'en';
	}
}
elseif (@preg_match('#ru#i', $_SERVER['HTTP_ACCEPT_LANGUAGE'] ?? ''))
{
	$lang = 'ru';
}
else
{
	$lang = 'en';
}

define("LANG", $lang);

if (LANG == 'ru')
{
	$msg = [
		'hello' => "Добро пожаловать!",
	];
}
else
{
	$msg = [
		'hello' => "Welcome!",
	];
}
?>
<!DOCTYPE html>
<html lang="<?= $lang ?>">
<head>
	<title><?= $msg['hello'] ?></title>
	<style>
        html,
        body {
            padding: 0 10px;
            margin: 0;
            background: #2fc6f7;
            position: relative;
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
            font-size: 16px;
        }
	</style>
</head>
<body>
	<h2>v.2.0023.523.1<h2>
</body>
</html>
