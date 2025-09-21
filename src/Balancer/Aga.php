<?php

namespace Wcore\Lbops\Balancer;

use Wcore\Lbops\Log;
use Aws\GlobalAccelerator\GlobalAcceleratorClient;

class Aga extends Abs
{
    /**
     * 配置
     *
     * @var array
     */
    public $config = [];

    /**
     * Undocumented variable
     *
     * @var GlobalAcceleratorClient
     */
    public $client;

    /**
     * 配置
     *
     * @param [type] $config
     */
    public function __construct($config)
    {
        $this->config = $config;

        $this->client = new GlobalAcceleratorClient([
            'credentials' => [
                'key' => $config['aws_key'],
                'secret' => $config['aws_secret'],
            ],
            'http' => [
                'connect_timeout' => 5,
                'timeout' => 15,
                'verify' => true, // Enable SSL/TLS verification for security
            ],
            'retries' => 3,
            'region' => 'us-west-2',
            'version' => '2018-08-08'
        ]);
    }

    /**
     * 获取当前发布的版本号
     *
     * @return string
     */
    public function getCurrentVersion()
    {
        $agaArn = reset($this->config['aga_arns']);

        //查询tag，找到发布的版本和时间戳
        try {
            $ret = $this->client->listTagsForResource([
                'ResourceArn' => $agaArn,
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to list tags for resource {$agaArn}: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $tags = $ret['Tags'] ?? [];
        $deployedVersion = null;
        foreach ($tags as $tag) {
            if ($tag['Key'] == "{$this->config['module']}:Deploy Version") {
                $deployedVersion = $tag['Value'];
                break;
            }
        }

        if (!$deployedVersion) {
            return [
                'suc' => true,
                'data' => '',
            ];
        }

        return [
            'suc' => true,
            'data' => $deployedVersion,
        ];
    }

    /**
     * 获取上次部署的时间
     */
    function getLastChangeDateTime()
    {
        if (!$this->config['aga_arns']) {
            return [
                'suc' => false,
                'msg' => "No aga arn",
            ];
        }

        $agaArn = reset($this->config['aga_arns']);

        //查询tag，找到发布的版本和时间戳
        try {
            $ret = $this->client->listTagsForResource([
                'ResourceArn' => $agaArn,
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to list tags for resource {$agaArn}: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $tags = $ret['Tags'] ?? [];
        $lastChangeTime = null;
        foreach ($tags as $tag) {
            if ($tag['Key'] == "{$this->config['module']}:Last Change DateTime") {
                $lastChangeTime = $tag['Value'];
                break;
            }
        }

        if (!$lastChangeTime) {
            return [
                'suc' => true,
                'data' => 0,
            ];
        }

        return [
            'suc' => true,
            'data' => $lastChangeTime,
        ];
    }

    /**
     * 更新tags
     */
    function updateTags($version = null)
    {
        if (!$this->config['aga_arns']) {
            return [
                'suc' => false,
                'msg' => "No aga arn",
            ];
        }

        //update tag
        Log::info("updating aga tags");

        //发布时间
        $now = new \DateTime('now', new \DateTimeZone('Asia/Shanghai'));
        $deployDatetime = $now->format('Y-m-d H:i:s');

        $removeTagKeys = ["{$this->config['module']}:Last Change DateTime"];
        $addTags = [
            [
                'Key' => "{$this->config['module']}:Last Change DateTime",
                'Value' => $deployDatetime,
            ]
        ];

        if ($version) {
            $removeTagKeys[] = "{$this->config['module']}:Deploy Version";
            $addTags[] = [
                'Key' => "{$this->config['module']}:Deploy Version",
                'Value' => $version,
            ];
        }

        foreach ($this->config['aga_arns'] as $agaArn) {
            try {
                $this->client->untagResource([
                    'ResourceArn' => $agaArn,
                    'TagKeys' => $removeTagKeys,
                ]);

                $this->client->tagResource([
                    'ResourceArn' => $agaArn,
                    'Tags' => $addTags
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to tag resource {$agaArn}: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }
        }

        Log::info("aga tags updated");

        return [
            'suc' => true,
        ];
    }

    /**
     * 获取所有地区的节点信息
     *
     * @param boolean $allInfo
     * @return void
     */
    public function getAllNodes($allInfo = false)
    {
        if (!$this->config['aga_arns']) {
            return [
                'suc' => false,
                'msg' => "No aga arn",
            ];
        }

        $agaArn = reset($this->config['aga_arns']);

        //找到第一个listener
        $ret = $this->listListenerArns($agaArn);
        if (!$ret['suc']) {
            return $ret;
        }
        $agaListeners = $ret['data'];
        $agaListenerArn = reset($agaListeners);

        try {
            $ret = $this->client->listEndpointGroups([
                'ListenerArn' => $agaListenerArn, // REQUIRED
                'MaxResults' => 30
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to list endpoint groups: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $data = [];
        foreach ($ret['EndpointGroups'] as $epg) {
            $region = $epg['EndpointGroupRegion'];

            $endpointList = array_filter($epg['EndpointDescriptions'], function ($tmpEp) {
                return $tmpEp['Weight'] > 0;
            });

            $instanceIds = array_column($endpointList, 'EndpointId');

            $currentRegionData = [];

            if ($allInfo) {
                //获取IP信息
                $ec2Client = new \Aws\Ec2\Ec2Client([
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
                    'region' => $region,
                    'version' => '2016-11-15'
                ]);

                try {
                    $ret = $ec2Client->describeInstances([
                        'InstanceIds' => $instanceIds
                    ]);
                } catch (\Exception $e) {
                    $errorMessage = "Failed to describe instances: {$e->getMessage()}";
                    Log::error($errorMessage);
                    return [
                        'suc' => false,
                        'msg' => $errorMessage,
                    ];
                }

                if ($ret && $ret['Reservations']) {
                    foreach ($ret['Reservations'] as $resv) {
                        foreach ($resv['Instances'] as $tmpIns) {
                            $currentRegionData[] = [
                                'ipv4' => $tmpIns['PublicIpAddress'],
                                'ipv6' => $tmpIns['Ipv6Address'],
                                'ins_id' => $tmpIns['InstanceId'],
                            ];
                        }
                    }
                }
            } else {
                $currentRegionData = array_map(function ($tmp) {
                    return [
                        'ins_id' => $tmp,
                    ];
                }, $instanceIds);
            }

            $data[$region] = $currentRegionData;
        }

        return [
            'suc' => true,
            'data' => $data,
        ];
    }

    /**
     * 从dns中获取指定地区地区对应的instance ids
     *
     * @param [type] $region
     * @return array
     */
    public function getNodesByRegion($region, $allInfo = false)
    {
        if (!$this->config['aga_arns']) {
            return [
                'suc' => false,
                'msg' => "No aga arn",
            ];
        }

        $agaArn = reset($this->config['aga_arns']);

        //找到第一个listener
        $ret = $this->listListenerArns($agaArn);
        if (!$ret['suc']) {
            return $ret;
        }
        $agaListeners = $ret['data'];
        $agaListenerArn = reset($agaListeners);

        try {
            $ret = $this->client->listEndpointGroups([
                'ListenerArn' => $agaListenerArn, // REQUIRED
                'MaxResults' => 30
            ]);
        } catch (\Exception $e) {
            return [
                'suc' => false,
                'msg' => "Failed to list endpoint groups: {$e->getMessage()}",
            ];
        }

        $regionNodes = [];
        foreach ($ret['EndpointGroups'] as $epg) {
            if ($epg['EndpointGroupRegion'] == $region) {
                $regionNodes = array_filter($epg['EndpointDescriptions'], function ($tmpEp) {
                    return $tmpEp['Weight'] > 0;
                });
                break;
            }
        }
        if (!$regionNodes) {
            return [
                'suc' => true,
                'data' => [],
            ];
        }

        $instanceIds = array_column($regionNodes, 'EndpointId');

        $data = [];
        if ($allInfo) {
            //获取IP信息
            $ec2Client = new \Aws\Ec2\Ec2Client([
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
                'region' => $region,
                'version' => '2016-11-15'
            ]);

            try {
                $ret = $ec2Client->describeInstances([
                    'InstanceIds' => $instanceIds
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to describe instances: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            if ($ret && $ret['Reservations']) {
                foreach ($ret['Reservations'] as $resv) {
                    foreach ($resv['Instances'] as $tmpIns) {
                        $data[] = [
                            'ipv4' => $tmpIns['PublicIpAddress'],
                            'ipv6' => $tmpIns['Ipv6Address'],
                            'ins_id' => $tmpIns['InstanceId'],
                        ];
                    }
                }
            }
        } else {
            $data = array_map(function ($tmp) {
                return [
                    'ins_id' => $tmp,
                ];
            }, $instanceIds);
        }

        return [
            'suc' => true,
            'data' => $data,
        ];
    }

    /**
     * 将endpoints部署到aga中
     */
    function replaceNodes($region, $insIdList)
    {
        if (!$this->config['aga_arns']) {
            return [
                'suc' => false,
                'msg' => "No aga arn",
            ];
        }

        if (!$insIdList) {
            return [
                'suc' => false,
                'msg' => "no ins_id list to replace",
            ];
        }

        Log::info("start replace instance in aga: " . implode(',', $insIdList));

        //循环部署所有的aga
        foreach ($this->config['aga_arns'] as $agaArn) {
            //找到第一个listener
            $ret = $this->listListenerArns($agaArn);
            if (!$ret['suc']) {
                return $ret;
            }
            $agaListeners = $ret['data'];
            $agaListenerArn = reset($agaListeners);

            //1. 添加
            $ret = $this->addEndpoints($agaListenerArn, $region, $insIdList);
            if (!$ret['suc']) {
                return $ret;
            }

            //2. 等待endpoint全部ready
            $ret = $this->waitEndpointsHealthy($agaListenerArn, $region, $insIdList);
            if (!$ret['suc']) {
                return $ret;
            }

            //3.将endpoint启用，weight改成128
            $ret = $this->enableEndpoints($agaListenerArn, $region, $insIdList);
            if (!$ret['suc']) {
                return $ret;
            }

            //4. 等待aga部署完成
            $ret = $this->waitAgaDeployed($agaArn);
            if (!$ret['suc']) {
                return $ret;
            }

            //5.删除旧节点 (仅保留新节点)
            Log::info("remove old endpoints in {$region}");
            $ret = $this->findEndpointGroupByRegion($agaListenerArn, $region);
            if (!$ret['suc']) {
                return $ret;
            }
            $epgInfo = $ret['data'];
            $newEndpointsConf = [];
            foreach ($epgInfo['EndpointDescriptions'] as $ep) {
                if (in_array($ep['EndpointId'], $insIdList)) {
                    $newEndpointsConf[] = $ep;
                }
            }

            try {
                $this->client->updateEndpointGroup([
                    'EndpointConfigurations' => $newEndpointsConf,
                    'EndpointGroupArn' => $epgInfo['EndpointGroupArn'], // REQUIRED
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to update endpoint group: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            Log::info("old endpoints removed in {$region}");
        }

        return [
            'suc' => true,
        ];
    }

    /**
     * 加到aga的endpoint中
     */
    public function addNodes($region, $insIdList)
    {
        Log::info("adding endpoints to aga in {$region}: " . implode(',', $insIdList));

        //循环部署所有的aga
        foreach ($this->config['aga_arns'] as $agaArn) {
            Log::info("start adding nodes to aga {$agaArn}");

            //找到第一个listener
            $ret = $this->listListenerArns($agaArn);
            if (!$ret['suc']) {
                return $ret;
            }
            $agaListeners = $ret['data'];
            $agaListenerArn = reset($agaListeners);

            //1. 添加
            $ret = $this->addEndpoints($agaListenerArn, $region, $insIdList);
            if (!$ret['suc']) {
                return $ret;
            }

            //2. 等待endpoint全部ready
            $ret = $this->waitEndpointsHealthy($agaListenerArn, $region, $insIdList);
            if (!$ret['suc']) {
                return $ret;
            }

            //3.将endpoint启用，weight改成128
            $ret = $this->enableEndpoints($agaListenerArn, $region, $insIdList);
            if (!$ret['suc']) {
                return $ret;
            }
        }

        Log::info("endpoints added to aga in {$region}: " . implode(',', $insIdList));

        return [
            'suc' => true,
        ];
    }

    /**
     * 添加endpoints
     */
    public function addEndpoints($agaListenerArn, $region, $insIdList)
    {

        //新的endpoints
        //https://docs.aws.amazon.com/aws-sdk-php/v3/api/api-globalaccelerator-2018-08-08.html#shape-endpointconfiguration
        $newEndpointConfs = array_map(function ($tmpInsId) {
            return [
                'ClientIPPreservationEnabled' => true,
                'EndpointId' => $tmpInsId,
                'Weight' => 0, //新节点都设权重为0，状态为healthy之后再改成128
            ];
        }, $insIdList);

        Log::info("adding endpoints to aga in {$region}: " . implode(',', $insIdList));

        $ret = $this->findEndpointGroupByRegion($agaListenerArn, $region);
        if (!$ret['suc']) {
            return $ret;
        }

        $epgInfo = $ret['data'] ?? [];

        if (!$epgInfo) {

            Log::info("no endpoint group in {$region}, create new one");

            $healthCheckParts = parse_url($this->config['health_check_url']);

            try {
                //该地区没有endpoint group，则创建一个
                $this->client->createEndpointGroup([
                    'EndpointConfigurations' => $newEndpointConfs,
                    'EndpointGroupRegion' => $region, // REQUIRED
                    'HealthCheckIntervalSeconds' => 10,
                    'ThresholdCount' => 2,
                    'HealthCheckPath' => $healthCheckParts['path'],
                    'HealthCheckPort' => 80,
                    'HealthCheckProtocol' => 'HTTP',
                    'IdempotencyToken' => $region . $agaListenerArn, // REQUIRED
                    'ListenerArn' => $agaListenerArn, // REQUIRED
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to create endpoint group: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            Log::info("success to create new endpoint group in {$region}");

            return [
                'suc' => true,
            ];
        }

        //有 endpoint group，则更新group并添加endpoints
        try {
            // 添加
            $this->client->updateEndpointGroup([
                'EndpointConfigurations' => array_merge($epgInfo['EndpointDescriptions'], $newEndpointConfs),
                'EndpointGroupArn' => $epgInfo['EndpointGroupArn'], // REQUIRED
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to update endpoint group: {$e->getMessage()}";
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
     * 等待endpoints healthy
     */
    public function waitEndpointsHealthy($agaListenerArn, $region, $insIdList)
    {
        Log::info("wait endpoint in aga to be healthy...");
        $counter = 1;
        $isHealthy = false;

        while (!$isHealthy && $counter <= 60) {

            $ret = $this->findEndpointGroupByRegion($agaListenerArn, $region);

            if (!$ret['suc']) {
                $errorMessage = $ret['msg'];
                Log::error($errorMessage);
                return $ret;
            }

            $epgInfo = $ret['data'];
            $healthyEndpoints = 0;
            foreach ($epgInfo['EndpointDescriptions'] as $ep) {
                if (in_array($ep['EndpointId'], $insIdList) && $ep['HealthState'] == 'HEALTHY') {
                    $healthyEndpoints++;
                }
            }

            //新节点全部healthy
            $isHealthy = $healthyEndpoints == count($insIdList);

            $counter++;
            sleep(5);
        }

        if (!$isHealthy) {
            $errorMessage = "endpoints in aga is not healthy after {$counter} checks:" . implode(',', $insIdList);
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        Log::info("endpoints in {$region} is healthy after {$counter} checks: " . implode(',', $insIdList));

        return [
            'suc' => true,
        ];
    }

    /**
     * 启用endpoints
     */
    public function enableEndpoints($agaListenerArn, $region, $insIdList)
    {
        //将endpoint启用（weight改成128）
        Log::info("enable endpoints in {$region}");

        $ret = $this->findEndpointGroupByRegion($agaListenerArn, $region);
        if (!$ret['suc']) {
            $errorMessage = $ret['msg'];
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }
        $epgInfo = $ret['data'];
        $newEndpointsConf = [];
        foreach ($epgInfo['EndpointDescriptions'] as $ep) {
            $tmpEp = $ep;
            if (in_array($ep['EndpointId'], $insIdList)) {
                $tmpEp['Weight'] = 128;
            }

            $newEndpointsConf[] = $tmpEp;
        }
        try {
            $this->client->updateEndpointGroup([
                'EndpointConfigurations' => $newEndpointsConf,
                'EndpointGroupArn' => $epgInfo['EndpointGroupArn'], // REQUIRED
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to update endpoint group: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        Log::info("endpoints enabled in {$region}");

        return [
            'suc' => true,
        ];
    }

    // /**
    //  * 从aga中删除endpoints
    //  */
    // function removeNodes($region, $amount)
    // {
    //     Log::info("removing {$amount} endpoints from aga in region {$region}");

    //     //获取现有的ins列表
    //     $nodesList = $this->getNodesByRegion($region);

    //     for ($i = 1; $i <= $amount; $i++) {
    //         $removeKey = array_rand($nodesList);
    //         unset($nodesList[$removeKey]);
    //     }

    //     if (!$nodesList) {
    //         Log::error("no nodes remains after removing {$amount} ips from route53 in {$region}, skip");
    //         return;
    //     }

    //     $insList = array_column($nodesList, 'ins_id');

    //     Log::info("remaining nodes in {$region}: " . implode(',', $insList));

    //     $newEndpointConfs = array_map(function ($tmpInsId) {
    //         return [
    //             'ClientIPPreservationEnabled' => true,
    //             'EndpointId' => $tmpInsId,
    //             'Weight' => 128,
    //         ];
    //     }, $insList);

    //     //循环部署所有的aga
    //     foreach ($this->config['aga_arns'] as $agaArn) {
    //         Log::info("start removes nodes from aga {$agaArn}");

    //         //找到第一个listener
    //         $agaListeners = $this->listListenerArns($agaArn);
    //         $agaListenerArn = reset($agaListeners);

    //         $epgInfo = $this->findEndpointGroupByRegion($agaListenerArn, $region);

    //         if (!$epgInfo) {
    //             Log::error("no endpoint group in {$region}");

    //             continue;
    //         }

    //         //更新
    //         $this->client->updateEndpointGroup([
    //             'EndpointConfigurations' => $newEndpointConfs,
    //             'EndpointGroupArn' => $epgInfo['EndpointGroupArn'], // REQUIRED
    //         ]);
    //     }

    //     Log::info("{$amount} endpoints removed from aga in region {$region}");

    //     return true;
    // }


    /**
     * 根据地区找到endpointgroup
     *
     * @param [type] $agaListenerArn
     * @param [type] $region
     * @return void
     */
    public function findEndpointGroupByRegion($agaListenerArn, $region)
    {
        try {
            $ret = $this->client->listEndpointGroups([
                'ListenerArn' => $agaListenerArn, // REQUIRED
                'MaxResults' => 30
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to list endpoint groups: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $epgInfo = null;
        foreach ($ret['EndpointGroups'] as $epg) {
            if ($epg['EndpointGroupRegion'] == $region) {
                $epgInfo = $epg;
                break;
            }
        }

        return [
            'suc' => true,
            'data' => $epgInfo,
        ];
    }

    /**
     * 获取aga的listener列表
     *
     * @param [type] $agaArn
     * @return array
     */
    public function listListenerArns($agaArn)
    {
        //取第一个listener
        try {
            $ret = $this->client->listListeners([
                'AcceleratorArn' => $agaArn,
                'MaxResults' => 10,
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to list listeners: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        if (!$ret || !$ret['Listeners']) {
            $errorMessage = "Can not find listeners for aga {$agaArn}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        return [
            'suc' => true,
            'data' => array_column($ret['Listeners'], 'ListenerArn'),
        ];
    }

    /**
     * 等待aga的状态变为deployed
     */
    function waitAgaDeployed($agaArn)
    {
        $counter = 1;

        Log::info("wait aga is deployed...");

        $isDeployed = false;

        while (!$isDeployed && $counter <= 60) {
            $counter++;
            sleep(5);

            //找到对应的endpint group
            try {
                $agaInfo = $this->client->describeAccelerator([
                    'AcceleratorArn' => $agaArn
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to describe accelerator: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            $isDeployed = $agaInfo['Accelerator']['Status'] == 'DEPLOYED';
        }

        if (!$isDeployed) {
            $errorMessage = "aga is not deployed after {$counter} checks";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        Log::info("aga is deployed after {$counter} checks");


        return [
            'suc' => true,
        ];
    }
}
