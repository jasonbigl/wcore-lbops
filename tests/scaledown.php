<?php
require __DIR__ . '/../vendor/autoload.php';

use Wcore\Lbops\Lbops;

class EchoLog
{
    public function info($message)
    {
        echo "info: " . $message . "\r\n";
    }
    public function error($message)
    {
        echo "error: " . $message . "\r\n";
    }
}


// export AWS_ACCESS_KEY_ID=""
// export AWS_SECRET_ACCESS_KEY=""
// export AWS_SUPPRESS_PHP_DEPRECATION_WARNING=true
$devopsClient = new Lbops([
    'module' => 'mc-dev', //系统模块名，区分多系统 !!!必须
    'regions' => [
        // "us-west-2",
        // "us-east-1",
        // "ap-southeast-1", //新加坡
        // "ap-south-1", //孟买
        // "eu-central-1", //法兰克福
        // "eu-west-2", //伦敦
        // "sa-east-1", //巴西圣保罗
        "ap-southeast-2", //悉尼
        // "af-south-1", //非洲开普敦
    ], //发布的地区 !!!必须

    'aws_key' => getenv('AWS_ACCESS_KEY_ID'), //aws key !!!必须 - Use environment variable
    'aws_secret' => getenv('AWS_SECRET_ACCESS_KEY'), //aws secret !!!必须 - Use environment variable

    'health_check_url' => 'https://demode.maxconv.top/.maxconv/health', //健康检查URL

    'launch_tpl' => 'maxconv-collector', //创建ec2的模板

    's3_startup_script' => 's3://maxconv-asset/collector/startup-ddb.sh', //开机脚本的s3位置
    's3_startup_script_region' => 'us-east-1', //开机脚本的s3桶的区域

    //需要发布的global accelerator列表，留空表示不发布
    'aga_arns' => [
        'arn:aws:globalaccelerator::471112600202:accelerator/a5f91893-f773-471a-869f-71d027338cdd',
    ],

    //需要发布的route53域名区，包含zone_id和domain, 留空表示不发布
    'r53_zones' => [
        [
            'zone_id' => 'Z018195015HSGNTU0ABO2',
            'domain' => 'maxconv.top'
        ]
    ],
    'r53_subdomain' => '*', // route53中的域名前缀，比如 *.domain.com就是*

    //日志
    'loggers' => [
        EchoLog::class
    ],
]);

//竖向扩容
$devopsClient->scaleDown('ap-southeast-2');
