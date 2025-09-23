<?php

namespace Wcore\Lbops;

use Wcore\Lbops\Balancer\Aga;
use Wcore\Lbops\Balancer\Route53;

class Basic
{
    /**
     * 默认aws配置
     *
     * @var [type]
     */
    public $defaultAwsConfig;

    /**
     * 配置
     *
     * @var array
     */
    public $config = [];

    /**
     * aga类
     *
     * @var [type]
     */
    public $aga;

    /**
     * route53类
     *
     * @var [type]
     */
    public $route53;

    /**
     * lock file location
     *
     * @var string
     */
    public $opLockFile;

    /**
     * Undocumented function
     *
     * @param [type] $config
     */
    public function __construct($config)
    {
        if (!$config['module']) {
            throw new \Exception('config.module is required');
        }

        if (!$config['regions']) {
            throw new \Exception('config.regions is required');
        }

        if (!$config['aws_key']) {
            throw new \Exception('config.aws_key is required');
        }

        if (!$config['aws_secret']) {
            throw new \Exception('config.aws_secret is required');
        }

        if ($config['health_check_url']) {
            $healthCheckParts = parse_url($config['health_check_url']);
            if (!$healthCheckParts['host'] || !$healthCheckParts['path']) {
                throw new \Exception("health check url is invalid: {$config['health_check_url']}");
            }
        }

        $defaultConfig = [
            'module' => '', //系统模块名，区分多系统 !!!必须
            'regions' => [], //发布的地区 !!!必须
            'aws_key' => '', //aws key !!!必须
            'aws_secret' => '', //aws secret !!!必须

            'health_check_url' => '', //健康检查的完整url，如 https://api.domain.com/.devops/health
            'launch_tpl' => '', //创建ec2的模板
            's3_startup_script' => '', //开机脚本的s3位置
            's3_startup_script_region' => '', //开机脚本的s3桶的区域

            'aga_arns' => [], //需要发布的global accelerator列表，留空表示不发布
            'r53_zones' => [], //需要发布的route53域名区，包含zone_id和domain, 留空表示不发布
            'r53_subdomain' => '', // route53中的域名前缀，比如 *.domain.com就是*

            'loggers' => [], //日志记录

            'email_from' => '',
            'email_to' => [],

            'op_lock_file' => $config['module'] ? "/tmp/wcore-lbops-{$config['module']}-op.lock" : "/tmp/wcore-lbops-op.lock",

            'region_min_nodes_amount' => [], //每个地区最少机器数目,默认最小为1

            // 'auto_scale_cpu_metric' => [],
            // 'auto_scale_cpu_threshold' => [], //缩容和扩容百分比
        ];

        $this->config = array_merge($defaultConfig, $config);

        $this->aga = new Aga($this->config);
        $this->route53 = new Route53($this->config);

        //记录日志
        if ($this->config['loggers']) {
            Log::setLoggers($this->config['loggers']);
        }

        //默认的aws 配置
        $this->defaultAwsConfig = [
            'credentials' => [
                'key' => $this->config['aws_key'],
                'secret' => $this->config['aws_secret'],
            ],
            'http' => [
                'connect_timeout' => 5,
                'timeout' => 15,
                'verify' => true, // Enable SSL/TLS verification for security
            ],
            'retries' => 3,
            'version' => 'latest',
        ];
    }

    /**
     * Undocumented function
     *
     * @param [type] $region
     * @param [type] $version
     * @param [type] $assignEIP 指定一个eip，而不是分配新eip
     * @param string $insType
     * @return string
     */
    function launchNode($region, $version, $insType = 't4g.small', $startupEnv = '')
    {
        #create instance
        $ret = $this->launchEc2($region, $version, $insType, $startupEnv);
        if (!$ret['suc']) {
            return $ret;
        }

        $instance = $ret['data'];

        $instanceId = $instance['InstanceId'];

        $ret = $this->waitInstanceReady($region, $instanceId);
        if (!$ret['suc']) {
            return [
                'suc' => false,
                'msg' => "Failed to wait instance ready, instance id: {$instanceId}",
            ];
        }

        $ret = $this->describeInstance($region, $instanceId);
        if (!$ret['suc']) {
            return $ret;
        }
        $instanceInfo = $ret['data'];
        $ipv4 = $instanceInfo['PublicIpAddress'];
        $ipv6 = $instanceInfo['Ipv6Address'];

        return [
            'suc' => true,
            'data' => [
                'ins_id' => $instanceId,
                'ipv4' => $ipv4,
                'ipv6' => $ipv6
            ]
        ];
    }

    /**
     * 启动新机器ec2
     *
     * @param [type] $region
     * @param [type] $regionAttr
     * @return void
     */
    public function launchEc2($region, $version, $insType, $startupEnv = '')
    {
        Log::info("launching ec2 instance, version: {$version}, region: {$region}");

        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        $userData = <<<STRING
#!/bin/bash

aws s3 cp {$this->config['s3_startup_script']} /tmp/startup.sh --region={$this->config['s3_startup_script_region']}

chmod +x /tmp/startup.sh

DEPLOY_FILE={$version}.tgz {$startupEnv} /tmp/startup.sh > /tmp/startup.log
STRING;

        try {
            $now = new \DateTime('now', new \DateTimeZone('Asia/Shanghai'));
            $deployDatetime = $now->format('Y-m-d H:i:s');

            $insOptions = [
                'MaxCount' => 1,
                'MinCount' => 1,
                'InstanceType' => $insType,
                'LaunchTemplate' => [
                    'LaunchTemplateName' => $this->config['launch_tpl'],
                    'Version' => '$Default',
                ],

                'DisableApiStop' => false,
                'DisableApiTermination' => false,

                'Monitoring' => [
                    'Enabled' => false,
                ],
                'TagSpecifications' => [
                    [
                        'ResourceType' => 'instance',
                        'Tags' => [
                            [
                                'Key' => 'Name',
                                'Value' => "{$this->config['module']}-{$version}",
                            ],
                            [
                                'Key' => 'Module',
                                'Value' => $this->config['module'],
                            ],
                            [
                                'Key' => 'Last Change DateTime',
                                'Value' => $deployDatetime,
                            ],
                            [
                                'Key' => 'Deploy Version',
                                'Value' => $version,
                            ]
                        ],
                    ],
                ],
                'UserData' => base64_encode($userData),
            ];

            if (stripos($insType, 't') === 0) {
                //cpu credits unlimited
                $insOptions['CreditSpecification'] = [
                    'CpuCredits' => 'unlimited'
                ];
            }

            $ret = $ec2Client->runInstances($insOptions);
        } catch (\Exception $e) {
            $errorMessage = "Failed to launch ec2 instance, error: {$e->getMessage()}";

            Log::error($errorMessage);

            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $instance = $ret['Instances'][0];

        Log::info("ec2 instance launched successfully, instance id: {$instance['InstanceId']}");

        return [
            'suc' => true,
            'data' => $instance,
        ];
    }

    /**
     * 等待instance创建完成
     */
    function waitInstanceReady($region, $instanceId)
    {
        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        $startTime = time();

        $ec2State = null;
        do {

            sleep(1);

            try {
                $ec2State = $ec2Client->describeInstanceStatus([
                    'IncludeAllInstances' => true,
                    'InstanceIds' => [$instanceId],
                ]);
            } catch (\Exception $e) {
                Log::error("Failed to describe instance status, error: {$e->getMessage()}");
                continue;
            }

            $ec2State = $ec2State['InstanceStatuses'][0]['InstanceState']['Name'] ?? '';
        } while ($ec2State != 'running' && time() - $startTime <= 60);

        if ($ec2State != 'running') {
            $timeUsed = time() - $startTime;
            return [
                'suc' => false,
                'msg' => "Instance is not running after {$timeUsed} seconds",
            ];
        }

        return [
            'suc' => true,
        ];
    }

    /**
     * 获取instance详情
     */
    public function describeInstance($region, $instanceId)
    {

        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        try {
            $ret = $ec2Client->describeInstances([
                'InstanceIds' => [$instanceId]
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to describe instance, error: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        if (empty($ret['Reservations']) || empty($ret['Reservations'][0]['Instances'])) {
            return [
                'suc' => false,
                'msg' => "No instance found with the given ID",
            ];
        }

        return [
            'suc' => true,
            'data' => $ret['Reservations'][0]['Instances'][0],
        ];
    }

    /**
     * 等待所有instance中的app启动完成
     *
     * @param [type] $ipList
     * @return void
     */
    public function waitAppReady($ipList)
    {
        Log::info("waiting app ready, ips: " . implode(',', $ipList));

        $maxAttempts = 30;

        $healthCheckParts = parse_url($this->config['health_check_url']);
        if (!$healthCheckParts['host'] || !$healthCheckParts['path']) {
            $errorMessage = "health check url is invalid: {$this->config['health_check_url']}";
            Log::error($errorMessage);

            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        //启动完成的nodes
        $readyNodes = [];
        $errorMsgs = [];
        foreach ($ipList as $ip) {
            $checkAttempts = 0;

            do {
                $ch = curl_init($this->config['health_check_url']);

                curl_setopt_array($ch, [
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_FOLLOWLOCATION => true,
                    CURLOPT_MAXREDIRS => 3,
                    CURLOPT_HEADER => false,
                    CURLOPT_FORBID_REUSE => false,
                    CURLOPT_TIMEOUT => 5,
                    CURLOPT_FAILONERROR => false, //ignore http status code
                    CURLOPT_RESOLVE => ["{$healthCheckParts['host']}:443:{$ip}"]
                ]);

                $checkAttempts++;

                try {
                    $body = curl_exec($ch);
                } catch (\Exception $e) {
                    curl_close($ch);

                    $errorMsgs[$ip] = "Failed to execute health check request, exception: {$e->getMessage()}";

                    sleep(5);

                    continue;
                }

                //有错误产生
                $errNo = curl_errno($ch);
                if ($errNo) {
                    $errMsg = curl_error($ch);

                    curl_close($ch);

                    $errorMsgs[$ip] = "Failed to execute health check request, error msg: {$errMsg}, error no: {$errNo}";

                    sleep(5);

                    continue;
                }

                $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                curl_close($ch);

                if ($httpCode === 200) {
                    //启动完成
                    $readyNodes[] = $ip;

                    Log::info("app in instances {$ip} is ready after {$checkAttempts} checks");

                    break;
                }

                $errorMsgs[$ip] = "Health check http code: {$httpCode}";

                sleep(5);
            } while ($checkAttempts <= $maxAttempts);
        }

        if (count($readyNodes) != count($ipList)) {
            $errorMessage = "app in instances is not ready after {$maxAttempts} checks, ready nodes: " . implode(',', $readyNodes) . "; all nodes:" . implode(',', $ipList) . "; error msgs: " . var_export($errorMsgs, true);

            Log::error($errorMessage);

            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        return [
            'suc' => true,
        ];
    }

    /**
     * 查询EIP的分配ID
     */
    function getAllocateID($region, $eip)
    {
        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        //allocate eip
        try {
            $ret = $ec2Client->describeAddresses([
                'Filters' => [
                    [
                        'Name' => 'public-ip',
                        'Values' => [$eip],
                    ],
                ],
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to get allocate id from existing EIP {$eip}, error: {$e->getMessage()}";
            Log::error($errorMessage);

            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $allocateId = $ret['Addresses'][0]['AllocationId'] ?? '';

        Log::info("Get allocate id from existing EIP {$eip}: {$allocateId}");

        return [
            'suc' => true,
            'data' => $allocateId,
        ];
    }

    /**
     * 分配EIP
     */
    function allocateNewEIP($region, $version)
    {
        $now = new \DateTime('now', new \DateTimeZone('Asia/Shanghai'));
        $deployDatetime = $now->format('Y-m-d H:i:s');

        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        //allocate eip
        try {
            $ret = $ec2Client->allocateAddress([
                'Domain' => 'vpc',
                'TagSpecifications' => [
                    [
                        'ResourceType' => 'elastic-ip',
                        'Tags' => [
                            [
                                'Key' => 'Name',
                                'Value' => "{$this->config['module']}-{$version}",
                            ],
                            [
                                'Key' => 'Module',
                                'Value' => $this->config['module'],
                            ],
                            [
                                'Key' => 'Last Change DateTime',
                                'Value' => $deployDatetime,
                            ],
                            [
                                'Key' => 'Deploy Version',
                                'Value' => $version,
                            ]
                        ]
                    ]
                ]
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to allocate new EIP, error: {$e->getMessage()}";
            Log::error($errorMessage);

            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $eip = $ret['PublicIp'];
        $allocId = $ret['AllocationId'];

        Log::info("allocate new eip:{$eip}, alloc id: {$allocId}");

        return [
            'suc' => true,
            'data' => [
                'allocate_id' => $allocId,
                'eip' => $eip,
            ]
        ];
    }

    /**
     * 关联eip
     */
    function associateEIP($region, $instanceId, $allocId)
    {
        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        //associate eip with instance
        try {
            $ret = $ec2Client->associateAddress([
                'AllocationId' => $allocId,
                'InstanceId' => $instanceId,
                'AllowReassociation' => true,
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to associate EIP {$allocId} with instance {$instanceId}, region: {$region}, error: {$e->getMessage()}";

            Log::error($errorMessage);

            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        Log::info("associate eip {$allocId} with instance {$instanceId} successfully");

        return [
            'suc' => true,
        ];
    }

    /**
     * 根据IP找到instance
     */
    function findInstanceByIP($region, $ip)
    {
        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        try {
            $ret = $ec2Client->describeInstances([
                'Filters' => [
                    [
                        'Name' => 'ip-address',
                        'Values' => [$ip]
                    ]
                ]
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to find instance by IP {$ip}, region: {$region}, error: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        if (empty($ret['Reservations']) || empty($ret['Reservations'][0]['Instances'])) {
            return [
                'suc' => false,
                'msg' => "No instance found with the given IP",
            ];
        }

        return [
            'suc' => true,
            'data' => $ret['Reservations'][0]['Instances'][0],
        ];
    }

    /**
     * 更新node的last change time
     */
    function updateNodeLastChangeTime($region, $insId, $lastChangeTime)
    {
        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        try {
            //先删除ec2 的tags
            $ec2Client->deleteTags([
                'Resources' => [$insId],
                'Tags' => [
                    [
                        'Key' => 'Last Change DateTime',
                    ]
                ]
            ]);

            //再添加ec2的tags
            $ec2Client->createTags([
                'Resources' => [$insId],
                'Tags' => [
                    [
                        'Key' => 'Last Change DateTime',
                        'Value' => $lastChangeTime,
                    ]
                ]
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to update node last change time, error: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }


        return [
            'suc' => true,
        ];
    }

    /**
     * 清理elastic eips
     */
    function cleanEIPs($region, $cleanEIPs)
    {
        if (!$cleanEIPs) {
            return [
                'suc' => true,
            ];
        }

        Log::info("Start cleaning eips in region {$region}: " . implode(',', $cleanEIPs));

        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        foreach ($cleanEIPs as $eip) {
            //eip
            try {
                $ret = $ec2Client->describeAddresses([
                    'Filters' => [
                        [
                            'Name' => 'public-ip',
                            'Values' => [$eip]
                        ]
                    ]
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to describe eip {$eip}, error: {$e->getMessage()}";
                Log::error($errorMessage);

                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            $eipInfo = null;
            if ($ret && $ret['Addresses']) {
                $eipInfo = $ret['Addresses'][0] ?? [];
            }

            if (!$eipInfo) {
                $errorMessage = "Can not find eip info from {$eip}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            Log::info("cleaning eip {$eip}...");

            if ($eipInfo['AssociationId']) {
                try {
                    $ec2Client->disassociateAddress([
                        'AssociationId' => $eipInfo['AssociationId'],
                    ]);
                    Log::info("disassociate {$eipInfo['AssociationId']} success");
                } catch (\Exception $e) {
                    $errorMessage = "disassociate {$eipInfo['AssociationId']} failed: " . $e->getMessage();
                    Log::error($errorMessage);
                    return [
                        'suc' => false,
                        'msg' => $errorMessage,
                    ];
                }
            }

            try {
                $ec2Client->releaseAddress([
                    'AllocationId' => $eipInfo['AllocationId']
                ]);
                Log::info("release {$eipInfo['AllocationId']} success");
            } catch (\Exception $e) {
                $errorMessage = "release {$eipInfo['AllocationId']} failed: " . $e->getMessage();
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            Log::info("clean eip {$eip} successfully");
        }

        return [
            'suc' => true
        ];
    }

    /**
     * 清理ec2
     */
    function cleanInstances($region, $cleanInsIds)
    {
        if (!$cleanInsIds) {
            return [
                'suc' => true,
            ];
        }

        $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
            'region' => $region,
            'version' => '2016-11-15'
        ]));

        Log::info("Start cleaning ec2 instances: " . json_encode($cleanInsIds, JSON_UNESCAPED_SLASHES));

        foreach ($cleanInsIds as $insId) {
            Log::info("cleaning ec2 instance {$insId}...");

            //分别取消停止保护和删除保护，不能同时取消，否则api报错
            try {
                $ret = $ec2Client->modifyInstanceAttribute([
                    'DisableApiStop' => ['Value' => false],
                    'InstanceId' => $insId,
                ]);
            } catch (\Throwable $th) {
                $errorMessage = "DisableApiStop failed: " . $th->getMessage();
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            try {
                $ret = $ec2Client->modifyInstanceAttribute([
                    'DisableApiTermination' => ['Value' => false],
                    'InstanceId' => $insId,
                ]);
            } catch (\Throwable $th) {
                $errorMessage = "DisableApiTermination failed: " . $th->getMessage();
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            //terminate
            try {
                $ret = $ec2Client->terminateInstances([
                    'InstanceIds' => [$insId],
                ]);
            } catch (\Throwable $th) {
                $errorMessage = $th->getMessage();
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            Log::info("ec2 instance {$insId} terminated successfully");
        }

        return [
            'suc' => true,
        ];
    }

    /**
     * 获取特定地区的所有机器
     *
     * @param [type] $region
     * @return array
     */
    function getNodesByRegion($region)
    {
        $regionNodes = [];
        if ($this->config['aga_arns']) {
            $ret = $this->aga->getNodesByRegion($region, true);
            if (!$ret['suc']) {
                Log::error("Failed to get nodes by region from aga, msg: {$ret['msg']}");
            }

            $regionNodes = $ret['data'] ?? [];
        }

        if (!$regionNodes && $this->config['r53_zones']) {
            $ret = $this->route53->getNodesByRegion($region, true);
            if (!$ret['suc']) {
                Log::error("Failed to get nodes by region from route53, msg: {$ret['msg']}");
            }

            $regionNodes = $ret['data'] ?? [];
        }

        if (!$regionNodes) {
            $errorMessage = "can not get nodes in region {$region}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        return [
            'suc' => true,
            'data' => $regionNodes
        ];
    }

    /**
     * 获取当前发布的版本号
     *
     * @return array
     */
    function getCurrentVersion()
    {
        //当前版本
        $currentVersion = null;
        if ($this->config['aga_arns']) {
            $ret = $this->aga->getCurrentVersion();
            if (!$ret['suc']) {
                $errorMessage = "Failed to get current version from aga, msg: {$ret['msg']}";
                Log::error($errorMessage);
            }

            $currentVersion = $ret['suc'] && $ret['data'] ? $ret['data'] : '';
        }

        if (!$currentVersion && $this->config['r53_zones']) {
            $ret = $this->route53->getCurrentVersion();
            if (!$ret['suc']) {
                $errorMessage = "Failed to get current version from route53, msg: {$ret['msg']}";
                Log::error($errorMessage);
            }
            $currentVersion = $ret['suc'] && $ret['data'] ? $ret['data'] : '';
        }

        if (!$currentVersion) {
            return [
                'suc' => false,
                'msg' => 'unable to get current version'
            ];
        }

        return [
            'suc' => true,
            'data' => $currentVersion
        ];
    }

    /**
     * 获取当前发布的instance type
     *
     * @return array
     */
    function getCurrentInstanceType($region = null)
    {
        if (!$region) {
            //如果没有指定region，以第一个地区为标准获取类型
            $region = reset($this->config['regions']);
        }

        $ret = $this->getNodesByRegion($region);
        if (!$ret) {
            return $ret;
        }
        $regionNodes = $ret['data'] ?? [];

        if (!$regionNodes) {
            $errorMessage = "can not get nodes in region {$region}, so no instance type to get, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //用第一个机器为标准获取类型
        $node = reset($regionNodes);
        $insId = $node['ins_id'];
        $ret = $this->describeInstance($region, $insId);
        if (!$ret['suc']) {
            $errorMessage = "Failed to describe instance in region {$region} so no instance type to get, msg: {$ret['msg']}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        $insType = $ret['data']['InstanceType'] ?? '';
        if (!$insType) {
            $errorMessage = "Failed to get instance type from instance {$insId} in region {$region}, so no instance type to get, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        return [
            'suc' => true,
            'data' => $insType
        ];
    }

    /**
     * 发送健康状况通知邮件
     *
     * @return void
     */
    public function sendAlarmEmail($subject, $content)
    {
        if (!$this->config['email_from'] || !$this->config['email_to']) {
            return;
        }

        $sesV2Client = new \Aws\SesV2\SesV2Client([
            'credentials' => [
                'key' => $this->config['aws_key'],
                'secret' => $this->config['aws_secret'],
            ],
            'http' => [
                'connect_timeout' => 5,
                'timeout' => 15,
                'verify' => true, // Enable SSL/TLS verification for security
            ],
            'retries' => 3,
            'version' => '2019-09-27',
            'region' => 'us-east-1'
        ]);

        $sesV2Client->sendEmail([
            'Content' => [
                'Simple' => [
                    'Body' => [
                        'Html' => [
                            'Charset' => 'UTF-8',
                            'Data' => $content,
                        ],
                    ],
                    'Subject' => [
                        'Charset' => 'UTF-8',
                        'Data' => $subject,
                    ],
                ]
            ],
            'Destination' => [
                'BccAddresses' => [],
                'CcAddresses' => [],
                //注意这里只能用单个用户，如果用多个用户，每个用户都能看到其他收件人的地址
                'ToAddresses' => $this->config['email_to'],
            ],
            'FromEmailAddress' => $this->config['email_from'],
        ]);
    }

    /**
     * Undocumented function
     *
     * @return boolean
     */
    public function opLocked($currentOp = '')
    {
        if (file_exists($this->config['op_lock_file']) && file_get_contents($this->config['op_lock_file']) != 'n') {
            $errorMessage = "trying {$currentOp} but lbops has been locked by another operation: " . file_get_contents($this->config['op_lock_file']) . ", skip";

            Log::info($errorMessage);

            return [
                'locked' => true,
                'msg' => $errorMessage
            ];
        }

        return [
            'locked' => false,
            'msg' => "lbops is not locked"
        ];
    }

    /**
     * lock operation
     *
     * @return void
     */
    public function lockOp($lockedBy = "common")
    {
        $ret = file_put_contents($this->config['op_lock_file'], $lockedBy);
        if ($ret === false) {
            return [
                'suc' => false,
                'msg' => "failed to lock operation"
            ];
        }

        return [
            'suc' => true
        ];
    }

    /**
     * unlock operation
     *
     * @return void
     */
    public function unlockOp()
    {
        file_put_contents($this->config['op_lock_file'], 'n');
    }

    /**
     * 确定是否由特定的操作来lock
     *
     * @param [type] $op
     * @return void
     */
    public function opLockedBy($op)
    {
        $lockedBy = null;
        $attempts = 0;
        do {
            if (file_exists($this->config['op_lock_file'])) {
                $lockedBy = file_get_contents($this->config['op_lock_file']);
            }

            $attempts++;
            sleep(1);
        } while ($lockedBy == $op && $attempts <= 5);

        if ($lockedBy == $op) {
            return [
                'suc' => true
            ];
        }

        return [
            'suc' => false,
            'msg' => "locked by another operation when double check lock file: {$lockedBy}"
        ];
    }
}
