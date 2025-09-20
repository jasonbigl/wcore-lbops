<?php

namespace Wcore\Lbops\Balancer;

use Aws\Route53\Route53Client;
use Wcore\Lbops\Log;

class Route53 extends Abs
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
     * @var Route53Client
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

        $this->client = new Route53Client([
            'credentials' => [
                'key' => $config['aws_key'],
                'secret' => $config['aws_secret'],
            ],
            'http' => [
                'connect_timeout' => 5,
                'timeout' => 15,
                'verify' => false, //Disable SSL/TLS verification
            ],
            'retries' => 3,
            'region' => 'us-east-1',
            'version' => '2013-04-01'
        ]);
    }

    /**
     * 获取当前发布的版本号
     *
     * @return string
     */
    public function getCurrentVersion()
    {
        $route53Zone = reset($this->config['r53_zones']);

        $route53ZoneId = $route53Zone['zone_id'];

        try {
            $ret = $this->client->listTagsForResource([
                'ResourceId' => $route53ZoneId,
                'ResourceType' => 'hostedzone',
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to list tags for resource {$route53ZoneId}: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $tags = $ret['ResourceTagSet']['Tags'];
        $deployedVersion = null;
        foreach ($tags as $tag) {
            if ($tag['Key'] == "{$this->config['module']}:Deploy Version") {
                $deployedVersion = $tag['Value'];
                break;
            }
        }

        if (!$deployedVersion) {
            Log::error("No deploy version in hostedzone tag");
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
        if (!$this->config['r53_zones']) {
            return [
                'suc' => false,
                'msg' => "No r53 zones",
            ];
        }

        //查询tag，找到发布的版本和时间戳
        $route53Zone = reset($this->config['r53_zones']);

        $route53ZoneId = $route53Zone['zone_id'];

        try {
            $ret = $this->client->listTagsForResource([
                'ResourceId' => $route53ZoneId,
                'ResourceType' => 'hostedzone',
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to list tags for resource {$route53ZoneId}: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $tags = $ret['ResourceTagSet']['Tags'] ?? [];
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
                'data' => '',
            ];
        }

        return [
            'suc' => true,
            'data' => $lastChangeTime,
        ];
    }

    /**
     * 更新dns tags
     *
     * @param [type] $eipv4
     * @param [type] $eipv6
     * @return void
     */
    public function updateTags($deployedVersion = null)
    {
        if (!$this->config['r53_zones']) {
            return [
                'suc' => false,
                'msg' => "No r53 zones",
            ];
        }

        //update route53 hosted zone tag
        Log::info("updating route53 tags");

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

        if ($deployedVersion) {
            $removeTagKeys[] = "{$this->config['module']}:Deploy Version";
            $addTags[] = [
                'Key' => "{$this->config['module']}:Deploy Version",
                'Value' => $deployedVersion,
            ];
        }

        foreach ($this->config['r53_zones'] as $dnsZone) {
            try {
                $this->client->changeTagsForResource([
                    'ResourceId' => $dnsZone['zone_id'],
                    'ResourceType' => 'hostedzone',
                    'RemoveTagKeys' => $removeTagKeys,
                    'AddTags' => $addTags
                ]);
            } catch (\Throwable $th) {
                $errorMessage = "Failed to change tags for resource {$dnsZone['zone_id']}: {$th->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }
        }

        Log::info("route53 tags updated");

        return [
            'suc' => true,
        ];
    }

    /**
     * 从dns中获取所有地区对应的所有ip列表（或者ip列表和ins列表）
     * return [
     *  'us-west-2'=>[
     *      [
     *          'ipv4'=>'', 'ipv6'=>'', 'ins_id'=>''
     *      ],
     *      ...
     *  ]
     * ]
     *
     * @return array
     */
    public function getAllNodes($allInfo = false)
    {
        if (!$this->config['r53_zones']) {
            return [
                'suc' => false,
                'msg' => "No r53 zones",
            ];
        }

        $route53Zone = reset($this->config['r53_zones']);

        $route53ZoneId = $route53Zone['zone_id'];
        $route53Domain = $route53Zone['domain'];

        // * 在返回结果中是 \052
        $subDomainInRecord = $this->config['r53_subdomain'] == '*' ? '\052' : $this->config['r53_subdomain'];

        try {
            $ret = $this->client->listResourceRecordSets([
                'HostedZoneId' => $route53ZoneId,
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to get dns records from route 53: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $data = [];
        foreach ($ret['ResourceRecordSets'] as $item) {
            if ($item['Type'] != 'A' || $item['Name'] != $subDomainInRecord . '.' . $route53Domain . '.') {
                continue;
            }

            $region = $item['Region'];

            $ec2Client = new \Aws\Ec2\Ec2Client([
                'credentials' => [
                    'key' => $this->config['aws_key'],
                    'secret' => $this->config['aws_secret'],
                ],
                'http' => [
                    'connect_timeout' => 5,
                    'timeout' => 15,
                    'verify' => false, //Disable SSL/TLS verification
                ],
                'retries' => 3,
                'region' => $region,
                'version' => '2016-11-15'
            ]);

            $currentRegionData = [];

            $ipList = array_column($item['ResourceRecords'], 'Value');

            if ($allInfo) {
                //同时返回ins id
                try {
                    $ret = $ec2Client->describeInstances([
                        'Filters' => [
                            [
                                'Name' => 'ip-address',
                                'Values' => $ipList
                            ]
                        ]
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
                        'ipv4' => $tmp,
                    ];
                }, $ipList);
            }

            $data[$region] = $currentRegionData;
        }

        return [
            'suc' => true,
            'data' => $data,
        ];
    }

    /**
     * 从dns中获取指定地区地区对应的ip列表（或者ip列表和ins列表）
     * return [
     *  [
     *      'ipv4'=>'', 'ipv6'=>'', 'ins_id'=>''
     *  ],
     *  ...
     * ]
     *
     * @return array
     */
    public function getNodesByRegion($region, $allInfo = false)
    {
        if (!$this->config['r53_zones']) {
            return [
                'suc' => false,
                'msg' => "No r53 zones",
            ];
        }

        $route53Zone = reset($this->config['r53_zones']);

        $route53ZoneId = $route53Zone['zone_id'];
        $route53Domain = $route53Zone['domain'];

        // * 在返回结果中是 \052
        $subDomainInRecord = $this->config['r53_subdomain'] == '*' ? '\052' : $this->config['r53_subdomain'];

        try {
            $ret = $this->client->listResourceRecordSets([
                'HostedZoneId' => $route53ZoneId,
            ]);
        } catch (\Exception $e) {
            $errorMessage = "Failed to get dns records from route 53: {$e->getMessage()}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage,
            ];
        }

        $ec2Client = new \Aws\Ec2\Ec2Client([
            'credentials' => [
                'key' => $this->config['aws_key'],
                'secret' => $this->config['aws_secret'],
            ],
            'http' => [
                'connect_timeout' => 5,
                'timeout' => 15,
                'verify' => false, //Disable SSL/TLS verification
            ],
            'retries' => 3,
            'region' => $region,
            'version' => '2016-11-15'
        ]);

        $data = [];
        foreach ($ret['ResourceRecordSets'] as $item) {
            if (!in_array($item['Type'], ['A']) || $item['Name'] != $subDomainInRecord . '.' . $route53Domain . '.') {
                continue;
            }

            if ($item['Region'] == $region) {

                $ipList = array_column($item['ResourceRecords'], 'Value');
                if ($allInfo) {

                    //同时返回ins id
                    try {
                        $ret = $ec2Client->describeInstances([
                            'Filters' => [
                                [
                                    'Name' => 'ip-address',
                                    'Values' => $ipList
                                ]
                            ]
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
                            'ipv4' => $tmp,
                        ];
                    }, $ipList);
                }

                break;
            }
        }

        return [
            'suc' => true,
            'data' => $data,
        ];
    }

    /**
     * 更新dns
     *
     * @param [type] $eipv4
     * @param [type] $eipv6
     * @return void
     */
    public function replaceNodes($region, $ipList)
    {
        if (!$ipList) {
            Log::error("no iplist to replace");
            return [
                'suc' => false,
                'msg' => "no iplist to replace",
            ];
        }

        //更新dns
        Log::info("updating dns in {$region}: " . implode(',', $ipList));

        //循环替换所有的zone
        foreach ($this->config['r53_zones'] as $dnsZone) {
            try {
                $ret = $this->client->changeResourceRecordSets([
                    'HostedZoneId' => $dnsZone['zone_id'],
                    'ChangeBatch' => [
                        'Changes' => [
                            [
                                'Action' => 'UPSERT',
                                'ResourceRecordSet' => [
                                    'Name' => "{$this->config['r53_subdomain']}.{$dnsZone['domain']}",
                                    'TTL' => 300,
                                    'Type' => 'A',
                                    'ResourceRecords' => array_map(function ($ip) {
                                        return [
                                            'Value' => $ip
                                        ];
                                    }, $ipList),
                                    'Region' => $region,
                                    'SetIdentifier' => "{$region}-a",
                                ]
                            ],
                        ]
                    ]
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to update dns: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            Log::info("dns updated successfully in {$region}: " . implode(',', $ipList));
        }

        return [
            'suc' => true,
        ];
    }

    /**
     * 往route53中新增ip
     *
     * @param [type] $region
     * @param [type] $ipList
     * @return void
     */
    function addNodes($region, $ipList)
    {
        if (!$ipList) {
            Log::error("no iplist to replace");
            return [
                'suc' => false,
                'msg' => "no iplist to replace",
            ];
        }

        Log::info("adding EIPs to DNS in region {$region}, ips: " . implode(',', $ipList));

        // * 在返回结果中是 \052
        $subDomainInRecord = $this->config['r53_subdomain'] == '*' ? '\052' : $this->config['r53_subdomain'];

        foreach ($this->config['r53_zones'] as $dnsZone) {
            $route53Domain = $dnsZone['domain'];
            $route53ZoneId = $dnsZone['zone_id'];

            try {
                $ret = $this->client->listResourceRecordSets([
                    'HostedZoneId' => $route53ZoneId,
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to get dns records from route 53: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            $dnsRecord = [];
            foreach ($ret['ResourceRecordSets'] as $item) {
                if ($item['Type'] == 'A' && $item['Name'] == $subDomainInRecord . '.' . $route53Domain . '.' && $item['Region'] == $region) {
                    $dnsRecord = $item;
                    break;
                }
            }

            // if (empty($dnsRecord)) {
            //     $errorMessage = "No existing dns record in route 53";
            //     Log::error($errorMessage);
            //     return [
            //         'suc' => false,
            //         'msg' => $errorMessage,
            //     ];
            // }

            //新增一个
            $resourceRecords = $dnsRecord ? $dnsRecord['ResourceRecords'] : [];
            foreach ($ipList as $newIp) {
                $resourceRecords[] = ['Value' => $newIp];
            }

            Log::info("update dns record in {$region}: " . json_encode($resourceRecords, JSON_UNESCAPED_SLASHES));

            $changePayload = [
                'HostedZoneId' => $route53ZoneId,
                'ChangeBatch' => [
                    'Changes' => [
                        [
                            'Action' => 'UPSERT',
                            'ResourceRecordSet' => [
                                'Name' => "{$this->config['r53_subdomain']}.{$route53Domain}",
                                'TTL' => 300,
                                'Type' => 'A',
                                'ResourceRecords' => $resourceRecords,
                                'Region' => $region,
                                'SetIdentifier' => "{$region}-a",
                            ]
                        ],
                    ]
                ]
            ];

            //更新
            try {
                $ret = $this->client->changeResourceRecordSets($changePayload);
            } catch (\Exception $e) {
                $errorMessage = "Failed to upate dns: {$e->getMessage()}";
                Log::error($errorMessage);
                return [
                    'suc' => false,
                    'msg' => $errorMessage,
                ];
            }

            Log::info("update dns record in {$region} success");
        }

        Log::info("EIPs have been successfully added to route53 zones in region {$region}");

        return [
            'suc' => true,
        ];
    }

    // /**
    //  * 从route53中移除
    //  *
    //  * @param [type] $region
    //  * @param [type] $amount 删除个数
    //  * @return void
    //  */
    // function removeNodes($region, $amount)
    // {
    //     Log::info("start removing {$amount} ips from route53 in {$region}");

    //     //获取现有的IP列表
    //     $nodesList = $this->getNodesByRegion($region);

    //     for ($i = 1; $i <= $amount; $i++) {
    //         $removeKey = array_rand($nodesList);
    //         unset($nodesList[$removeKey]);
    //     }

    //     if (!$nodesList) {
    //         Log::error("no nodes remains after removing {$amount} ips from route53 in {$region}, skip");
    //         return;
    //     }

    //     $ipList = array_column($nodesList, 'ipv4');

    //     Log::info("remaining nodes in {$region}: " . implode(',', $ipList));

    //     $remainingIPsRecords = array_map(function ($tmp) {
    //         return ['Value' => $tmp];
    //     }, $ipList);

    //     foreach ($this->config['r53_zones'] as $dnsZone) {
    //         $route53Domain = $dnsZone['domain'];
    //         $route53ZoneId = $dnsZone['zone_id'];

    //         $changePayload = [
    //             'HostedZoneId' => $route53ZoneId,
    //             'ChangeBatch' => [
    //                 'Changes' => [
    //                     [
    //                         'Action' => 'UPSERT',
    //                         'ResourceRecordSet' => [
    //                             'Name' => "{$this->config['r53_subdomain']}.{$route53Domain}",
    //                             'TTL' => 300,
    //                             'Type' => 'A',
    //                             'ResourceRecords' => $remainingIPsRecords,
    //                             'Region' => $region,
    //                             'SetIdentifier' => "{$region}-a",
    //                         ]
    //                     ],
    //                 ]
    //             ]
    //         ];

    //         //更新DNS
    //         try {
    //             $this->client->changeResourceRecordSets($changePayload);
    //         } catch (\Exception $e) {
    //             Log::error($e->getMessage());
    //             return false;
    //         }
    //     }

    //     Log::info("{$amount} ips successfully removed from DNS in {$region}");

    //     return true;
    // }
}
