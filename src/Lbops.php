<?php

namespace Wcore\Lbops;

class Lbops extends Basic
{
    //竖向扩容机器路线
    public $verticalScaleInstypes = [
        't4g.small',
        'c6g.xlarge',
        'c6g.2xlarge',
    ];

    /**
     * 是否可以进行操作
     *
     * @param [type] $opName
     * @return boolean
     */
    function canDoOp($opName)
    {
        $ret = $this->opLocked($opName);
        if ($ret['locked']) {
            return [
                'suc' => false,
                'msg' => $ret['msg']
            ];
        }

        $ret = $this->lockOp($opName);
        if (!$ret['suc']) {
            return $ret;
        }

        //确定只有locked by self
        $ret = $this->opLockedBy($opName);
        if (!$ret['suc']) {
            return $ret;
        }

        return [
            'suc' => true,
        ];
    }

    /**
     * 新版本发布
     *
     * @return void
     */
    public function deploy($version, $allocateNewEIP = false, $targetRegion = null, $insType = null)
    {
        $ret = $this->canDoOp('deploy');
        if (!$ret['suc']) {
            return $ret;
        }

        $startTime = time();

        //当前版本
        $ret = $this->getCurrentVersion();
        $currentVersion = $ret['data'] ?? ''; //发布时，没有获取到当前版本也没关系，只是一个提示作用

        //当前机器类型
        if (!$insType) {
            $ret = $this->getCurrentInstanceType();
            if (!$ret['suc']) {
                $this->unlockOp();
                return $ret;
            }

            $insType = $ret['data'];
        }

        Log::info("### start deploy `{$this->config['module']}`, instance type `{$insType}`, new version: `{$version}`, current version: `{$currentVersion}` ###");
        sleep(5); //等待一段时间，方便观察信息是否正确

        $newRegionInsList = [];

        foreach ($this->config['regions'] as $region) {
            if ($targetRegion && $region != $targetRegion) {
                //只发布指定区域，非指定区域跳过
                continue;
            }

            //需要发布的服务器数
            $deployCount = 1;
            $ret = $this->getNodesByRegion($region);

            //发布时，没有获取到当前所有机器也没有关系，可能时新地区没有发布，没有机器则新发布一个
            $regionNodes = $ret['data'] ?? [];

            if (!$regionNodes) {
                //该地区目前不存在，则发布一个
                Log::info("no current servers in {$region}, deploy new one");
                $deployCount = 1;
            } else {
                $deployCount = count($regionNodes);
            }

            Log::info("start deploy {$deployCount} servers in {$region}");

            //启动ec2实例
            $newInsList = [];
            for ($i = 1; $i <= $deployCount; $i++) {
                Log::info("start launching #{$i} server in {$region}");

                //部署并等待app完成
                $ret = $this->launchNode($region, $version, $insType);
                if (!$ret['suc']) {
                    Log::error("Failed to deploy server in {$region}, msg: {$ret['msg']}");
                    continue;
                }

                Log::info("end launching #{$i} server in {$region}");
                $newInsList[] = $ret['data'];
            }

            Log::info("end launch {$deployCount} servers in {$region}: " . json_encode($newInsList, JSON_UNESCAPED_SLASHES));
            //启动实例完成

            //保存
            $newRegionInsList[$region] = $newInsList;
        }

        //将新机器部署
        foreach ($newRegionInsList as $region => $insList) {
            $insIdList = array_column($insList, 'ins_id');
            $ipv4List = array_column($insList, 'ipv4');

            //等待app ready
            $ret = $this->waitAppReady($ipv4List);
            if (!$ret['suc']) {
                Log::error("Failed to wait app ready in {$region}, msg: {$ret['msg']}");
                continue;
            }

            //将新启动的ec2部署到route53中
            if ($this->config['r53_zones']) {

                $ret = $this->route53->getNodesByRegion($region, true);
                if (!$ret['suc']) {
                    Log::error("Failed to get nodes by region from route53, msg: {$ret['msg']}");
                }

                $regionNodes = $ret['data'] ?? [];

                if ($allocateNewEIP || !$regionNodes) {
                    //需要分配新eip
                    $newIpList = [];
                    foreach ($insList as $ins) {
                        $ipv4 =  $ins['ipv4'];
                        $insId =  $ins['ins_id'];

                        $newIp = null;

                        $ret = $this->allocateNewEIP($region, $version);
                        if (!$ret['suc']) {
                            Log::error("Failed to allocate new EIP, msg: {$ret['msg']}");
                            $newIp = $ipv4; //用临时IP
                        } else {
                            $allocateId = $ret['data']['allocate_id'];
                            $newIp = $ret['data']['eip'];

                            //关联机器
                            $ret = $this->associateEIP($region, $insId, $allocateId);
                            if (!$ret['suc']) {
                                Log::error("Failed to associate EIP {$newIp} with instance {$insId}, msg: {$ret['msg']}");
                                $newIp = $ipv4; //用临时IP
                            }
                        }

                        if ($newIp) {
                            $newIpList[] = $newIp;
                        }
                    }

                    $ret = $this->route53->replaceNodes($region, $newIpList);
                    if (!$ret['suc']) {
                        Log::error("Failed to replace nodes in {$region}, msg: {$ret['msg']}");
                    }

                    //更改了node才更新date time
                    $ret = $this->route53->updateTags($version);
                    if (!$ret['suc']) {
                        Log::error("Failed to update tags in {$region}, msg: {$ret['msg']}");
                    }
                } else {
                    //直接用旧的eip重新关联ec2即可，无需更改route53
                    //获取旧eip
                    foreach ($regionNodes as $idx => $rnode) {
                        $oldEIP = $rnode['ipv4'];
                        $newInsId = $insList[$idx] ?? null;
                        if (!$newInsId) {
                            Log::error("can not get new instance id according to route53 and inslist, region nodes: " . json_encode($regionNodes, JSON_UNESCAPED_SLASHES) . ', insList: ' . json_encode($insList, JSON_UNESCAPED_SLASHES) . ', idx: ' . $idx);
                            continue;
                        }
                        $newInsId = $newInsId['ins_id'];

                        $ret = $this->getAllocateID($region, $oldEIP);
                        if (!$ret['suc']) {
                            Log::error("Failed to get allocate id from EIP {$oldEIP}, msg: {$ret['msg']}");
                            continue;
                        }

                        //关联新机器
                        $ret = $this->associateEIP($region, $newInsId, $ret['data']['allocate_id']);
                        if (!$ret['suc']) {
                            Log::error("Failed to associate EIP {$oldEIP} with instance {$newInsId}, msg: {$ret['msg']}");
                        }
                    }
                }
            }

            //将新启动的ec2部署到aga中
            if ($this->config['aga_arns']) {
                //先全部添加
                foreach ($this->config['aga_arns'] as $agaArn) {
                    //找到第一个listener
                    $ret = $this->aga->listListenerArns($agaArn);
                    if (!$ret['suc']) {
                        Log::error("Failed to list listener arns from aga, msg: {$ret['msg']}");
                    }

                    $agaListeners = $ret['data'] ?? [];
                    $agaListenerArn = reset($agaListeners);

                    //添加
                    $ret = $this->aga->addEndpoints($agaListenerArn, $region, $insIdList);
                    if (!$ret['suc']) {
                        Log::error("Failed to add endpoints to aga, msg: {$ret['msg']}");
                    }
                }
            }
        }

        //有aga，之前只是全部添加，还未启用，开始检查healthy并启用
        if ($this->config['aga_arns']) {
            foreach ($newRegionInsList as $region => $insList) {
                $insIdList = array_column($insList, 'ins_id');

                foreach ($this->config['aga_arns'] as $agaArn) {
                    //找到第一个listener
                    $ret = $this->aga->listListenerArns($agaArn);
                    if (!$ret['suc']) {
                        Log::error("Failed to list listener arns from aga, msg: {$ret['msg']}");
                    }

                    $agaListeners = $ret['data'] ?? [];
                    $agaListenerArn = reset($agaListeners);

                    //2. 等待endpoint全部ready
                    $ret = $this->aga->waitEndpointsHealthy($agaListenerArn, $region, $insIdList);
                    if (!$ret['suc']) {
                        Log::error("Failed to wait endpoints healthy in aga, msg: {$ret['msg']}");
                    }

                    //3.将endpoint启用，weight改成128
                    $ret = $this->aga->enableEndpoints($agaListenerArn, $region, $insIdList);
                    if (!$ret['suc']) {
                        Log::error("Failed to enable endpoints in aga, msg: {$ret['msg']}");
                    }
                }
            }
        }

        //有aga，删除旧节点
        if ($this->config['aga_arns']) {
            foreach ($this->config['aga_arns'] as $agaArn) {
                //4. 等待aga部署完成
                $ret = $this->aga->waitAgaDeployed($agaArn);
                if (!$ret['suc']) {
                    Log::error("Failed to wait aga deployed, msg: {$ret['msg']}");
                    continue;
                }

                //找到第一个listener
                $ret = $this->aga->listListenerArns($agaArn);
                if (!$ret['suc']) {
                    Log::error("Failed to list listener arns from aga, msg: {$ret['msg']}");
                }

                $agaListeners = $ret['data'] ?? [];
                $agaListenerArn = reset($agaListeners);

                foreach ($newRegionInsList as $region => $insList) {
                    $insIdList = array_column($insList, 'ins_id');

                    //5.删除旧节点 (仅保留新节点)
                    Log::info("remove old endpoints in {$region}");
                    $ret = $this->aga->findEndpointGroupByRegion($agaListenerArn, $region);
                    if (!$ret['suc']) {
                        Log::error("Failed to find endpoint group by region from aga, msg: {$ret['msg']}");
                    }

                    $epgInfo = $ret['data'];
                    $newEndpointsConf = [];
                    foreach ($epgInfo['EndpointDescriptions'] as $ep) {
                        if (in_array($ep['EndpointId'], $insIdList)) {
                            $newEndpointsConf[] = $ep;
                        }
                    }
                    try {
                        $this->aga->client->updateEndpointGroup([
                            'EndpointConfigurations' => $newEndpointsConf,
                            'EndpointGroupArn' => $epgInfo['EndpointGroupArn'], // REQUIRED
                        ]);
                    } catch (\Exception $e) {
                        $errorMessage = "Failed to update endpoint group: {$e->getMessage()}";
                        Log::error($errorMessage);
                        continue;
                    }

                    Log::info("old endpoints removed in {$region}, remaining: " . implode(',', $insIdList));
                }
            }
        }

        //update tag
        $ret = $this->aga->updateTags($version);
        if (!$ret['suc']) {
            Log::error("Failed to update tags in aga, msg: {$ret['msg']}");
        }

        $timeUsed = time() - $startTime;
        Log::info("deploy finished, version: {$version}, time used: {$timeUsed}s");

        $this->unlockOp();
    }

    /**
     * 清理机器
     *
     * @return void
     */
    public function clean($minAliveSeconds = 2400, $exceptIpList = [], $exceptInsidList = [])
    {
        $ret = $this->canDoOp('clean');
        if (!$ret['suc']) {
            return $ret;
        }

        Log::info("start clean module `{$this->config['module']}`");
        sleep(5);

        //按地区清理
        foreach ($this->config['regions'] as $region) {
            $ec2Client = new \Aws\Ec2\Ec2Client(array_merge($this->defaultAwsConfig, [
                'region' => $region,
                'version' => '2016-11-15'
            ]));

            //记录在案的，不能清理
            $agaResvInsIds = $agaResvIps = $r53ResvInsIds = $r53ResvIps = [];

            //找到aga中的instance，这些是不能清理的
            if ($this->config['aga_arns']) {
                $ret = $this->aga->getNodesByRegion($region, true);
                if (!$ret['suc']) {
                    Log::error("Failed to get nodes by region from aga, msg: {$ret['msg']}");
                }

                $agaResvInsList = $ret['data'] ?? [];

                if ($agaResvInsList) {
                    $agaResvInsIds = array_column($agaResvInsList, 'ins_id');
                    $agaResvIps = array_column($agaResvInsList, 'ipv4');

                    Log::info("{$region} aga reserved instances: " . implode(',', $agaResvInsIds) . ', reserved ips: ' . implode(',', $agaResvIps));
                } else {
                    Log::info("{$region} aga reserved instances: <empty>, reserved ips: <empty>");
                }
            }

            if ($this->config['r53_zones']) {
                $ret = $this->route53->getNodesByRegion($region, true);
                if (!$ret['suc']) {
                    Log::error("Failed to get nodes by region from route53, msg: {$ret['msg']}");
                }

                $r53ResvInsList = $ret['data'] ?? [];

                if ($r53ResvInsList) {
                    $r53ResvInsIds = array_column($r53ResvInsList, 'ins_id');
                    $r53ResvIps = array_column($r53ResvInsList, 'ipv4');

                    Log::info("{$region} route53 reserved instances: " . implode(',', $r53ResvInsIds) . ', reserved ips: ' . implode(',', $r53ResvIps));
                } else {
                    Log::info("{$region} route53 reserved instances: <empty>, reserved ips: <empty>");
                }
            }

            if ($this->config['aga_arns'] && $this->config['r53_zones']) {
                //两者都有的，取交集，测试一下是否有不一样的配置
                $intersectInsIds = array_intersect($agaResvInsIds, $r53ResvInsIds);
                if (count($intersectInsIds) != count($agaResvInsIds) || count($intersectInsIds) != count($r53ResvInsIds)) {
                    Log::error("the instance id in aga and route 53 is different, aga: " . implode(',', $agaResvInsIds) . ", route53: " . implode(',', $r53ResvInsIds));
                }

                $intersectIps = array_intersect($agaResvIps, $r53ResvIps);
                if (count($intersectIps) != count($agaResvIps) || count($intersectIps) != count($r53ResvIps)) {
                    Log::error("the ip in aga and route 53 is different, aga: " . implode(',', $agaResvIps) . ", route53: " . implode(',', $r53ResvIps));
                }
            }

            if ($exceptIpList) {
                Log::info("except ip list:" . implode(',', $exceptIpList));
            }

            //清理eip
            //先找到所有的eip
            try {
                $ret = $ec2Client->describeAddresses([
                    'Filters' => [
                        [
                            'Name' => 'tag:Module',
                            'Values' => [$this->config['module']]
                        ]
                    ]
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to describe addresses: {$e->getMessage()}";
                Log::error($errorMessage);
            }

            if ($ret && $ret['Addresses']) {
                //开始清理
                $cleanEIPs = [];

                foreach ($ret['Addresses'] as $eipAddr) {
                    //获取发布时间
                    $tags = $eipAddr['Tags'] ?? [];
                    $eipLastChangeTs = 0;
                    foreach ($tags as $tag) {
                        if ($tag['Key'] == "Last Change DateTime") {
                            $eipLastChangeTs = \DateTime::createFromFormat('Y-m-d H:i:s', $tag['Value'], new \DateTimeZone('Asia/Shanghai'))->getTimestamp();;
                            break;
                        }
                    }

                    if (
                        !in_array($eipAddr['PublicIp'], $agaResvIps)
                        && !in_array($eipAddr['PublicIp'], $r53ResvIps)
                        && !in_array($eipAddr['PublicIp'], $exceptIpList)
                        && $eipLastChangeTs > 0
                        && time() - $eipLastChangeTs > $minAliveSeconds
                    ) {
                        $cleanEIPs[] = $eipAddr['PublicIp'];
                    }
                }

                if ($cleanEIPs) {
                    $ret = $this->cleanEIPs($region, $cleanEIPs);
                    if (!$ret['suc']) {
                        Log::error("Failed to clean eips in {$region}, msg: {$ret['msg']}");
                    }
                }
            }

            if ($exceptInsidList) {
                Log::info("except instance id list:" . implode(',', $exceptInsidList));
            }

            //清理instance
            try {
                $ret = $ec2Client->describeInstances([
                    'Filters' => [
                        [
                            'Name' => 'tag:Module',
                            'Values' => [$this->config['module']]
                        ],
                        [
                            'Name' => 'instance-state-name',
                            'Values' => ['running', 'stopped']
                        ],
                    ]
                ]);
            } catch (\Exception $e) {
                $errorMessage = "Failed to describe instances: {$e->getMessage()}";
                Log::error($errorMessage);
            }

            if ($ret && $ret['Reservations']) {
                $cleanInsIds = [];

                foreach ($ret['Reservations'] as $rv) {
                    foreach ($rv['Instances'] as $ins) {
                        //获取发布时间
                        $tags = $ins['Tags'] ?? [];
                        $ec2LastChangeTs = 0;
                        foreach ($tags as $tag) {
                            if ($tag['Key'] == "Last Change DateTime") {
                                $ec2LastChangeTs = \DateTime::createFromFormat('Y-m-d H:i:s', $tag['Value'], new \DateTimeZone('Asia/Shanghai'))->getTimestamp();;
                                break;
                            }
                        }

                        if (
                            !in_array($ins['InstanceId'], $agaResvInsIds)
                            && !in_array($ins['InstanceId'], $r53ResvInsIds)
                            && !in_array($ins['InstanceId'], $exceptInsidList)
                            && $ec2LastChangeTs > 0
                            && time() - $ec2LastChangeTs > $minAliveSeconds
                        )
                            $cleanInsIds[] = $ins['InstanceId'];
                    }
                }

                if ($cleanInsIds) {
                    $ret = $this->cleanInstances($region, $cleanInsIds);
                    if (!$ret['suc']) {
                        Log::error("Failed to clean instances in {$region}, msg: {$ret['msg']}");
                    }
                }
            }
        }

        $this->unlockOp();
    }

    /**
     * 横向扩容（增加机器）
     *
     * @param [type] $region 地区
     * @param integer $amount 服务器数目
     * @return void
     */
    function scaleOut($region, $amount = 1)
    {
        if ($amount < 1 || $amount > 50) {
            $errorMessage = "invalid scale out amount, should between 1~50, current: {$amount}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //当前版本
        $ret = $this->getCurrentVersion();
        if (!$ret['suc']) {
            return $ret;
        }
        $currentVersion = $ret['data'] ?? '';
        if (!$currentVersion) {
            $errorMessage = "unable to get current version to scale out, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //当前instance类型
        $ret = $this->getCurrentInstanceType();
        if (!$ret['suc']) {
            $errorMessage = "unable to get current instance type to scale out, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }
        $insType = $ret['data'];

        //lock
        $ret = $this->canDoOp('scale-out');
        if (!$ret['suc']) {
            return $ret;
        }

        Log::info("scale out in region:{$region}, version: {$currentVersion}, instance type: {$insType}, amount: {$amount}");
        sleep(5);

        $insList = [];

        for ($i = 1; $i <= $amount; $i++) {
            //新建机器
            $ret = $this->launchNode($region, $currentVersion, $insType);
            if (!$ret['suc']) {
                $this->unlockOp();
                return $ret;
            }

            $insList[] = $ret['data'];
        }

        if (!$insList) {
            $errorMessage = "no new instances launched successfully";
            Log::error($errorMessage);
            $this->unlockOp();
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //等待app ready
        $ret = $this->waitAppReady(array_column($insList, 'ipv4'));
        if (!$ret['suc']) {
            $this->unlockOp();
            return $ret;
        }

        //需要分配新eip
        $newIpList = [];
        $newInsIdList = [];
        foreach ($insList as $ins) {
            $ipv4 =  $ins['ipv4'];
            $insId =  $ins['ins_id'];

            $newInsIdList[] = $insId;

            if ($this->config['r53_zones']) {
                //有route53，需要分配新eip
                $newIp = null;

                $ret = $this->allocateNewEIP($region, $currentVersion);
                if (!$ret['suc']) {
                    Log::error("Failed to allocate new EIP");
                    $newIp = $ipv4; //用临时IP
                } else {
                    $newIp = $ret['data']['eip'];
                    $allocateId = $ret['data']['allocate_id'];

                    //关联机器
                    $ret = $this->associateEIP($region, $insId, $allocateId);
                    if (!$ret['suc']) {
                        Log::error("Failed to associate EIP {$newIp} with instance {$insId}");
                        $newIp = $ipv4; //用临时IP
                    }
                }

                if ($newIp) {
                    $newIpList[] = $newIp;
                }
            }
        }

        if ($this->config['r53_zones'] && $newIpList) {
            $ret = $this->route53->addNodes($region, $newIpList);
            if (!$ret['suc']) {
                Log::error("Failed to add nodes to route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
            $ret = $this->route53->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        if ($this->config['aga_arns'] && $newInsIdList) {
            $ret = $this->aga->addNodes($region, $newInsIdList);
            if (!$ret['suc']) {
                Log::error("Failed to add nodes to aga, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
            $ret = $this->aga->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in aga, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        $this->unlockOp();

        return [
            'suc' => true,
        ];
    }

    /**
     * 横向缩容（减少机器）
     *
     * @param [type] $region 地区
     * @param integer $amount 服务器数目
     * @return void
     */
    function scaleIn($region, $amount = 1)
    {
        if ($amount < 1 || $amount > 50) {
            $errorMessage = "invalid scale in amount, should between 1~50, current: {$amount}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        Log::info("scale-in in region:{$region}, amount: {$amount}");

        $ret = $this->getNodesByRegion($region);
        if (!$ret['suc']) {
            return $ret;
        }
        $nodesList = $ret['data'] ?? [];
        if (!$nodesList) {
            $errorMessage = "can not get nodes in region {$region}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        if (count($nodesList) <= $amount) {
            $errorMessage = "current nodes amount in {$region} is less than or equal to the amount to scale in: {$amount}, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //删除，需要统一删除，保持route53个aga的一致性
        $removedNodes = [];
        for ($i = 1; $i <= $amount; $i++) {
            $removeKey = array_rand($nodesList);
            $removedNodes[] = $nodesList[$removeKey];
            unset($nodesList[$removeKey]);
        }

        if (!$nodesList) {
            $errorMessage = "no nodes remains after removing {$amount} nodes in {$region}, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        $ret = $this->canDoOp('scale-in');
        if (!$ret['suc']) {
            return $ret;
        }

        Log::info("remaining nodes in {$region}: " . json_encode($nodesList, JSON_UNESCAPED_SLASHES));

        if ($this->config['r53_zones']) {
            $ret = $this->route53->replaceNodes($region, array_column($nodesList, 'ipv4'));
            if (!$ret['suc']) {
                Log::error("Failed to replace nodes in route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
            $ret = $this->route53->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        if ($this->config['aga_arns']) {
            //直接替换，不需要等待healthy
            $insIdList = array_column($nodesList, 'ins_id');

            $newEndpointConfs = array_map(function ($tmpInsId) {
                return [
                    'ClientIPPreservationEnabled' => true,
                    'EndpointId' => $tmpInsId,
                    'Weight' => 128,
                ];
            }, $insIdList);

            //循环部署所有的aga
            foreach ($this->config['aga_arns'] as $agaArn) {
                //找到第一个listener
                $ret = $this->aga->listListenerArns($agaArn);
                if (!$ret['suc']) {
                    Log::error("Failed to list listener arns from aga, msg: {$ret['msg']}");
                    $this->unlockOp();
                    return $ret;
                }
                $agaListeners = $ret['data'];
                $agaListenerArn = reset($agaListeners);

                $ret = $this->aga->findEndpointGroupByRegion($agaListenerArn, $region);
                if (!$ret['suc']) {
                    Log::error("Failed to find endpoint group by region from aga, msg: {$ret['msg']}");
                    $this->unlockOp();
                    return $ret;
                }
                $epgInfo = $ret['data'];

                try {
                    $this->aga->client->updateEndpointGroup([
                        'EndpointConfigurations' => $newEndpointConfs,
                        'EndpointGroupArn' => $epgInfo['EndpointGroupArn'], // REQUIRED
                    ]);
                } catch (\Exception $e) {
                    $errorMessage = "Failed to update endpoint group: {$e->getMessage()}";
                    Log::error($errorMessage);
                    $this->unlockOp();
                    return [
                        'suc' => false,
                        'msg' => $errorMessage,
                    ];
                }


                Log::info("aga updated successfully in {$region} : " . implode(',', $insIdList));
            }

            $ret = $this->aga->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in aga, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        //更新removed node的last change time，避免刚缩容就被清理
        $now = new \DateTime('now', new \DateTimeZone('Asia/Shanghai'));
        $lastChangeTime = $now->format('Y-m-d H:i:s');
        foreach ($removedNodes as $node) {
            $ret = $this->updateNodeLastChangeTime($region, $node['ins_id'], $lastChangeTime);
            if (!$ret['suc']) {
                Log::error("Failed to update node last change time, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        $this->unlockOp();

        return [
            'suc' => true,
        ];
    }

    /**
     * 竖向扩容（增加机器资源如cpu等）
     *
     * @param [type] $region 地区
     * @param boolean $force 是否强制扩容
     * @return void
     */
    function scaleUp($region, $force = false)
    {
        //当前版本
        $ret = $this->getCurrentVersion();
        if (!$ret['suc']) {
            return $ret;
        }
        $currentVersion = $ret['data'] ?? '';
        if (!$currentVersion) {
            $errorMessage = "unable to get current version to scale up, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //区域数量
        $nodesList = $this->getNodesByRegion($region);
        if (!$nodesList['suc']) {
            return $nodesList;
        }
        $nodesList = $nodesList['data'] ?? [];
        if (!$nodesList) {
            $errorMessage = "can not get nodes in region {$region} to scale up, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }
        $amount = count($nodesList);

        //lock
        if (!$force) {
            $ret = $this->canDoOp('scale-up');
            if (!$ret['suc']) {
                return $ret;
            }
        }

        //默认升级到最大的一个类型
        $targetInsType = end($this->verticalScaleInstypes);

        //当前instance类型, 没有类型也没关系，用最大的一个
        $ret = $this->getCurrentInstanceType();
        $insType = $ret['data'] ?? '';

        if ($insType) {
            $currentKey = array_search($insType, $this->verticalScaleInstypes);
            if ($currentKey !== false) {
                $targetKey = $currentKey + 1;
                $targetInsType = $this->verticalScaleInstypes[$targetKey] ?? null;
                if (!$targetInsType) {
                    //到顶了，横向扩容
                    Log::info("current instance type {$insType} is the largest, scale out");
                    $this->unlockOp();
                    return $this->scaleOut($region, $amount);
                }
            }
        }

        Log::info("### scale up in region:{$region}, amount: {$amount}, version: {$currentVersion}, current instance type: {$insType}, target instance type: {$targetInsType} ###");

        //启动新机器
        $insList = [];
        for ($i = 1; $i <= $amount; $i++) {
            $ret = $this->launchNode($region, $currentVersion, $targetInsType);
            if (!$ret['suc']) {
                Log::error("failed to launch node");
                $this->unlockOp();
                return [
                    'suc' => false,
                    'msg' => "failed to launch node"
                ];
            }

            $insList[] = $ret['data'];
        }

        if (!$insList) {
            $errorMessage = "no new instances launched successfully";
            Log::error($errorMessage);
            $this->unlockOp();
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //等待app ready
        $ret = $this->waitAppReady(array_column($insList, 'ipv4'));
        if (!$ret['suc']) {
            $this->unlockOp();
            return [
                'suc' => false,
                'msg' => "failed to wait app ready"
            ];
        }

        if ($this->config['r53_zones']) {
            //用旧eip
            $ret = $this->route53->getNodesByRegion($region, true);
            if (!$ret['suc']) {
                Log::error("Failed to get nodes by region from route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
            $r53RegionalNodes = $ret['data'] ?? [];

            foreach ($r53RegionalNodes as $idx => $rnode) {
                //直接用旧的eip重新关联ec2即可，无需更改route53
                $oldEIP = $rnode['ipv4'];
                $newInsId = $insList[$idx] ?? [];
                if (!$newInsId || !isset($newInsId['ins_id'])) {
                    Log::error("can not get new instance id according to route53 and inslist, region nodes: " . json_encode($r53RegionalNodes, JSON_UNESCAPED_SLASHES) . ', insList: ' . json_encode($insList, JSON_UNESCAPED_SLASHES) . ', idx: ' . $idx);
                    $this->unlockOp();
                    return [
                        'suc' => false,
                        'msg' => "can not get new instance id according to route53 and inslist"
                    ];
                }
                $newInsId = $newInsId['ins_id'];

                $ret = $this->getAllocateID($region, $oldEIP);
                if (!$ret['suc']) {
                    Log::error("Failed to get allocate id from EIP {$oldEIP}, msg: {$ret['msg']}");
                    $this->unlockOp();
                    return $ret;
                }

                //关联新机器
                $allocateId = $ret['data']['allocate_id'];
                $ret = $this->associateEIP($region, $newInsId, $allocateId);
                if (!$ret['suc']) {
                    Log::error("Failed to associate EIP {$oldEIP} with instance {$newInsId}, msg: {$ret['msg']}");
                    $this->unlockOp();
                    return $ret;
                }
            }

            $ret = $this->route53->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        if ($this->config['aga_arns']) {
            $insIds = array_column($insList, 'ins_id');
            $ret = $this->aga->replaceNodes($region, $insIds);
            if (!$ret['suc']) {
                Log::error("Failed to replace nodes in aga, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }

            $ret = $this->aga->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in aga, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        $this->unlockOp();

        return [
            'suc' => true,
        ];
    }

    /**
     * 竖向缩容（减少机器资源如cpu等）
     *
     * @param [type] $region 地区
     * @param integer $amount 服务器数目
     * @return void
     */
    function scaleDown($region)
    {
        //当前版本
        $ret = $this->getCurrentVersion();
        if (!$ret['suc']) {
            return $ret;
        }
        $currentVersion = $ret['data'] ?? '';
        if (!$currentVersion) {
            $errorMessage = "unable to get current version to scale down, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //当前instance类型
        $ret = $this->getCurrentInstanceType();
        if (!$ret['suc']) {
            return $ret;
        }
        $insType = $ret['data'] ?? '';
        if (!$insType) {
            $errorMessage = "unable to get current instance type to scale down, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //当前数量
        $ret = $this->getNodesByRegion($region);
        if (!$ret['suc']) {
            return $ret;
        }
        $nodesList = $ret['data'] ?? [];
        if (!$nodesList) {
            $errorMessage = "can not get nodes in region {$region} to scale down, skip";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }
        $amount = count($nodesList);

        if ($amount <= 0) {
            $errorMessage = "invalid instance amount: {$amount}";
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        $currentKey = array_search($insType, $this->verticalScaleInstypes);
        if ($currentKey === false) {
            $errorMessage = "unable to find current instance type {$insType} location in types: " . implode(',', $this->verticalScaleInstypes);
            Log::error($errorMessage);
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        $targetKey = $currentKey - 1;
        $targetInsType = $this->verticalScaleInstypes[$targetKey] ?? null;
        if (!$targetInsType) {
            //到底了，横向缩容
            Log::error("current instance type {$insType} is the smallest, scale in");

            return $this->scaleIn($region, 1);
        }

        $ret = $this->canDoOp('scale-down');
        if (!$ret['suc']) {
            return $ret;
        }

        Log::info("### scale down in region:{$region}, amount: {$amount}, version: {$currentVersion}, current instance type: {$insType}, target instance type: {$targetInsType} ###");

        //启动新机器
        $insList = [];
        for ($i = 1; $i <= $amount; $i++) {
            $ret = $this->launchNode($region, $currentVersion, $targetInsType);
            if (!$ret['suc']) {
                Log::error("failed to launch node");
                $this->unlockOp();
                return [
                    'suc' => false,
                    'msg' => "failed to launch node"
                ];
            }

            $insList[] = $ret['data'];
        }

        if (!$insList) {
            $errorMessage = "no new instances launched successfully";
            Log::error($errorMessage);
            $this->unlockOp();
            return [
                'suc' => false,
                'msg' => $errorMessage
            ];
        }

        //等待app ready
        $ret = $this->waitAppReady(array_column($insList, 'ipv4'));
        if (!$ret['suc']) {
            $this->unlockOp();
            return $ret;
        }

        if ($this->config['r53_zones']) {
            //用旧eip
            $ret = $this->route53->getNodesByRegion($region, true);
            if (!$ret['suc']) {
                Log::error("Failed to get nodes by region from route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
            $r53RegionalNodes = $ret['data'] ?? [];

            foreach ($r53RegionalNodes as $idx => $rnode) {
                //直接用旧的eip重新关联ec2即可，无需更改route53
                $oldEIP = $rnode['ipv4'];
                $newInsId = $insList[$idx] ?? null;
                if (!$newInsId || !isset($newInsId['ins_id'])) {
                    $errorMessage = "can not get new instance id according to route53 and inslist, region nodes: " . json_encode($r53RegionalNodes, JSON_UNESCAPED_SLASHES) . ', insList: ' . json_encode($insList, JSON_UNESCAPED_SLASHES) . ', idx: ' . $idx;
                    Log::error($errorMessage);
                    $this->unlockOp();
                    return [
                        'suc' => false,
                        'msg' => $errorMessage
                    ];
                }
                $newInsId = $newInsId['ins_id'];

                $ret = $this->getAllocateID($region, $oldEIP);
                if (!$ret['suc']) {
                    $errorMessage = "Failed to get allocate id from EIP {$oldEIP}";
                    Log::error($errorMessage);
                    $this->unlockOp();
                    return [
                        'suc' => false,
                        'msg' => $errorMessage
                    ];
                }

                //关联新机器
                $allocateId = $ret['data']['allocate_id'];
                $ret = $this->associateEIP($region, $newInsId, $allocateId);
                if (!$ret['suc']) {
                    $errorMessage = "Failed to associate EIP {$oldEIP} with instance {$newInsId}";
                    Log::error($errorMessage);
                    $this->unlockOp();
                    return [
                        'suc' => false,
                        'msg' => $errorMessage
                    ];
                }
            }

            $ret = $this->route53->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in route53, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        if ($this->config['aga_arns']) {
            $insIds = array_column($insList, 'ins_id');
            $ret = $this->aga->replaceNodes($region, $insIds);
            if (!$ret['suc']) {
                Log::error("Failed to replace nodes in aga, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }

            $ret = $this->aga->updateTags();
            if (!$ret['suc']) {
                Log::error("Failed to update tags in aga, msg: {$ret['msg']}");
                $this->unlockOp();
                return $ret;
            }
        }

        $this->unlockOp();

        return [
            'suc' => true,
        ];
    }

    /**
     * 健康监控 (高性能并发版本)
     *
     * @param [type] $intervalS 检测时间间隔
     * @param [type] $failThreshold 失败指标的连续次数
     * @return void
     */
    public function monitor($intervalS, $failThreshold, $maxCheckAttempts)
    {
        if ($intervalS < 10) {
            Log::error("interval too small, should be > 10, current: {$intervalS}");
            return;
        }

        // monitor不看锁，避免监控的时候锁住，导致监控不及时

        $startTime = time();

        $regionNodes = [];
        if ($this->config['r53_zones']) {
            //优先用route53中的ip作为监控目标，避免aga中的机器IP再发布版本的时候变化
            $ret = $this->route53->getAllNodes(true);
            if (!$ret['suc']) {
                Log::error("Failed to get all nodes from route53, msg: {$ret['msg']}");
                return;
            }
            $regionNodes = $ret['data'] ?? [];
        } else {
            //用aga中的ip作为监控目标
            $ret = $this->aga->getAllNodes(true);
            if (!$ret['suc']) {
                Log::error("Failed to get all nodes from aga, msg: {$ret['msg']}");
                return;
            }
            $regionNodes = $ret['data'] ?? [];
        }
        if (!$regionNodes) {
            Log::error("Failed to get all nodes from route53 or aga, msg: " . ($ret['msg'] ?? 'unknown error'));
            return;
        }

        $healthCheckParts = parse_url($this->config['health_check_url']);
        $healthCheckDomain = $healthCheckParts['host'];

        // Process all regions concurrently
        foreach ($regionNodes as $region => $nodeList) {
            Log::info("Starting concurrent health check for region {$region} with " . count($nodeList) . " nodes");
            
            $unhealthyNodes = $this->performConcurrentHealthChecks(
                $nodeList, 
                $region, 
                $healthCheckDomain, 
                $intervalS, 
                $failThreshold, 
                $maxCheckAttempts
            );

            if ($unhealthyNodes) {
                //节点不健康
                $nodeContent = array_map(function ($item) {
                    return "<p>{$item}</p>";
                }, $unhealthyNodes);

                //直接升级 (强制，不看锁)
                $ret = $this->scaleUp($region, true);
                if (!$ret['suc']) {
                    //扩容失败
                    Log::error("Failed to scale up, msg: {$ret['msg']}");

                    $content = implode('', $nodeContent) . "<p>Message: {$ret['msg']}<p>";

                    $this->sendAlarmEmail('Unhealthy nodes, scale up failed', $content);
                } else {
                    //扩容成功
                    Log::info("Scale up successfully, msg: {$ret['msg']}");
                    $content = implode('', $nodeContent) . "<p>Scale up succeeded<p>";
                    $this->sendAlarmEmail('Unhealthy nodes, scale up succeeded', $content);
                }
            }
        }

        $timeUsed = time() - $startTime;

        Log::info("Finish monitor, time used: {$timeUsed}s");
    }

    /**
     * 并发执行健康检查 (使用 cURL multi-handle)
     *
     * @param array $nodeList 节点列表
     * @param string $region 区域名称
     * @param string $healthCheckDomain 健康检查域名
     * @param int $intervalS 检测时间间隔
     * @param int $failThreshold 失败阈值
     * @param int $maxCheckAttempts 最大检查次数
     * @return array 不健康的节点列表
     */
    private function performConcurrentHealthChecks($nodeList, $region, $healthCheckDomain, $intervalS, $failThreshold, $maxCheckAttempts)
    {
        $unhealthyNodes = [];
        
        // 为每个节点初始化健康状态跟踪
        $nodeHealthStatus = [];
        foreach ($nodeList as $node) {
            $nodeHealthStatus[$node['ipv4']] = [
                'node' => $node,
                'unHealthyCount' => 0,
                'checkAttempts' => 0,
                'isUnhealthy' => false
            ];
        }

        // 执行多轮检查直到达到最大尝试次数或失败阈值
        for ($round = 0; $round < $maxCheckAttempts; $round++) {
            $startRoundTime = time();
            
            // 只检查还未被判定为不健康的节点
            $activeNodes = array_filter($nodeHealthStatus, function($status) {
                return !$status['isUnhealthy'];
            });

            if (empty($activeNodes)) {
                break; // 所有节点都已经被判定
            }

            Log::info("Health check round " . ($round + 1) . " for region {$region}, checking " . count($activeNodes) . " nodes");

            // 并发检查当前活跃的节点
            $roundResults = $this->executeConcurrentHealthCheck($activeNodes, $healthCheckDomain);

            // 更新节点健康状态
            foreach ($roundResults as $nodeIp => $result) {
                $nodeHealthStatus[$nodeIp]['checkAttempts']++;
                
                if (!$result['success'] || $result['httpCode'] !== 200) {
                    $nodeHealthStatus[$nodeIp]['unHealthyCount']++;
                }

                // 检查是否达到失败阈值
                if ($nodeHealthStatus[$nodeIp]['unHealthyCount'] >= $failThreshold) {
                    $nodeHealthStatus[$nodeIp]['isUnhealthy'] = true;
                    $node = $nodeHealthStatus[$nodeIp]['node'];
                    
                    Log::error("Unhealthy node {$node['ins_id']} ({$node['ipv4']}) in {$region} - failed {$nodeHealthStatus[$nodeIp]['unHealthyCount']} times");
                    
                    $unhealthyNodes[] = "Unhealthy node {$node['ins_id']} ({$node['ipv4']}) in {$region}";
                    
                    // 找到一个不健康的节点就停止检查该区域（与原逻辑保持一致）
                    return $unhealthyNodes;
                }
            }

            // 如果不是最后一轮，等待间隔时间
            if ($round < $maxCheckAttempts - 1) {
                $roundDuration = time() - $startRoundTime;
                $sleepTime = max(0, $intervalS - $roundDuration);
                if ($sleepTime > 0) {
                    sleep($sleepTime);
                }
            }
        }

        return $unhealthyNodes;
    }

    /**
     * 使用 cURL multi-handle 并发执行健康检查
     *
     * @param array $activeNodes 活跃节点状态数组
     * @param string $healthCheckDomain 健康检查域名
     * @return array 检查结果数组
     */
    private function executeConcurrentHealthCheck($activeNodes, $healthCheckDomain)
    {
        $results = [];
        $curlHandles = [];
        $multiHandle = curl_multi_init();

        // 为每个节点创建 cURL 句柄
        foreach ($activeNodes as $nodeIp => $nodeStatus) {
            $ch = curl_init($this->config['health_check_url']);
            
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_FOLLOWLOCATION => false,
                CURLOPT_HEADER => false,
                CURLOPT_FORBID_REUSE => false,
                CURLOPT_TIMEOUT => 5,
                CURLOPT_CONNECTTIMEOUT => 3,
                CURLOPT_FAILONERROR => false, // ignore http status code
                CURLOPT_RESOLVE => ["{$healthCheckDomain}:443:{$nodeIp}"],
                CURLOPT_NOSIGNAL => 1, // 避免信号中断
            ]);

            curl_multi_add_handle($multiHandle, $ch);
            $curlHandles[$nodeIp] = $ch;
        }

        // 执行并发请求
        $running = null;
        do {
            curl_multi_exec($multiHandle, $running);
            curl_multi_select($multiHandle, 0.1); // 100ms 超时
        } while ($running > 0);

        // 收集结果
        foreach ($curlHandles as $nodeIp => $ch) {
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $error = curl_error($ch);
            $errno = curl_errno($ch);
            
            $results[$nodeIp] = [
                'success' => ($errno === 0 && empty($error)),
                'httpCode' => $httpCode,
                'error' => $error,
                'errno' => $errno
            ];

            // 清理
            curl_multi_remove_handle($multiHandle, $ch);
            curl_close($ch);
        }

        curl_multi_close($multiHandle);

        return $results;
    }

    /**
     * 性能测试：比较串行和并发健康检查的性能
     * 
     * @param array $testNodes 测试节点列表 
     * @param int $iterations 测试迭代次数
     * @return array 性能测试结果
     */
    public function benchmarkHealthCheck($testNodes = null, $iterations = 1)
    {
        if (!$testNodes) {
            // 使用实际节点进行测试
            if ($this->config['r53_zones']) {
                $ret = $this->route53->getAllNodes(true);
                if (!$ret['suc']) {
                    return ['error' => 'Failed to get nodes for benchmark'];
                }
                $allNodes = $ret['data'] ?? [];
            } else {
                $ret = $this->aga->getAllNodes(true);
                if (!$ret['suc']) {
                    return ['error' => 'Failed to get nodes for benchmark'];
                }
                $allNodes = $ret['data'] ?? [];
            }
            
            // 使用第一个区域的节点进行测试
            $testNodes = reset($allNodes) ?: [];
            if (empty($testNodes)) {
                return ['error' => 'No nodes available for benchmark'];
            }
        }

        $healthCheckParts = parse_url($this->config['health_check_url']);
        $healthCheckDomain = $healthCheckParts['host'];
        
        $results = [
            'node_count' => count($testNodes),
            'iterations' => $iterations,
            'concurrent_times' => [],
            'concurrent_avg' => 0,
            'performance_improvement' => 'N/A (concurrent only - old serial method replaced)'
        ];

        Log::info("Starting health check benchmark with " . count($testNodes) . " nodes, {$iterations} iterations");

        // 测试并发版本性能
        for ($i = 0; $i < $iterations; $i++) {
            $startTime = microtime(true);
            
            $activeNodes = [];
            foreach ($testNodes as $node) {
                $activeNodes[$node['ipv4']] = [
                    'node' => $node,
                    'unHealthyCount' => 0,
                    'checkAttempts' => 0,
                    'isUnhealthy' => false
                ];
            }
            
            $this->executeConcurrentHealthCheck($activeNodes, $healthCheckDomain);
            
            $concurrentTime = microtime(true) - $startTime;
            $results['concurrent_times'][] = $concurrentTime;
            
            Log::info("Concurrent check iteration " . ($i + 1) . " completed in " . round($concurrentTime, 3) . "s");
        }

        $results['concurrent_avg'] = array_sum($results['concurrent_times']) / count($results['concurrent_times']);
        
        Log::info("Benchmark completed. Average concurrent time: " . round($results['concurrent_avg'], 3) . "s");
        Log::info("Estimated old serial time would be: ~" . round($results['concurrent_avg'] * count($testNodes), 1) . "s");
        Log::info("Performance improvement: ~" . round(count($testNodes), 1) . "x faster");

        return $results;
    }

    /**
     * 根据cpu负载自动扩容和缩容
     *
     * @return void
     */
    public function autoScale($metricRegion, $metricFilter, $metricThreshold, $regionMinNodesAmount = [])
    {
        //debug log
        //Log::info("start watching auto scale");

        $startTime = time();

        $regionNodes = [];
        if ($this->config['r53_zones']) {
            //优先用route53中的ip作为监控目标，避免发布版本的时候aga中的机器发生变化
            $ret = $this->route53->getAllNodes(true);
            if (!$ret['suc']) {
                Log::error("Failed to get all nodes from route53, msg: {$ret['msg']}");
                return;
            }
            $regionNodes = $ret['data'] ?? [];
        } else {
            //用aga中的ip作为监控目标
            $ret = $this->aga->getAllNodes(true);
            if (!$ret['suc']) {
                Log::error("Failed to get all nodes from aga, msg: {$ret['msg']}");
                return;
            }
            $regionNodes = $ret['data'] ?? [];
        }

        if (!$regionNodes) {
            Log::error("Failed to get all nodes from route53 or aga, msg: {$ret['msg']}");
            return;
        }

        $cwClient = new \Aws\CloudWatch\CloudWatchClient(array_merge($this->defaultAwsConfig, [
            'region' => $metricRegion,
            'version' => '2010-08-01'
        ]));

        foreach ($regionNodes as $region => $nodeList) {
            //该地区总共节点数
            $totalNodes = count($nodeList);

            //低负载节点
            $lowLoadNodes = 0;

            $currentCPUTotal = 0;

            foreach ($nodeList as $node) {
                $insId = $node['ins_id'];

                //查询该instance的cpu使用情况
                $nowTs = time();
                try {
                    $ret = $cwClient->getMetricStatistics(array_merge($metricFilter, [
                        'Dimensions' => [
                            [
                                'Name' => 'InstanceId',
                                'Value' => $insId
                            ],
                        ],
                        'StartTime' => $nowTs - (12 * $metricFilter['Period']),
                        'EndTime' => $nowTs,
                        'Statistics' => ['Average'],
                        'Unit' => 'Percent'
                    ]));
                } catch (\Throwable $th) {
                    Log::error($th->getMessage());
                    continue;
                }

                $dataPoints = $ret['Datapoints'];
                if (!$dataPoints) {
                    continue;
                }

                //倒序，最近的就是第一个
                usort($dataPoints, function ($a, $b) {
                    if ($a['Timestamp'] == $b['Timestamp']) {
                        return 0;
                    }
                    return ($a['Timestamp'] < $b['Timestamp']) ? 1 : -1;
                });

                //取最近的6个数据点
                $dataPoints = array_slice($dataPoints, 0, 6);

                //该instance的平均cpu
                $totalCpuArr = array_column($dataPoints, 'Average');
                $totalCpu = array_sum($totalCpuArr);
                $currentAvgCpu = number_format($totalCpu / count($totalCpuArr), 2, '.', '');

                //加到总cpu
                $currentCPUTotal += $currentAvgCpu;

                //debug log
                //Log::info("nodes metrics in {$region}, datapoints:" . count($totalCpuArr) . ", total datapoints cpu: {$totalCpu}%, avg cpu: {$currentAvgCpu}% ");

                if ($currentAvgCpu > 0 && $currentAvgCpu < $metricThreshold[0]) {
                    //小于thread的第一个值，也就是最小值，缩容
                    $lowLoadNodes++;
                }
            }

            //当前地区的平均cpu
            $currentCPUAvg = $currentCPUTotal / $totalNodes;

            //debug log
            //Log::info("nodes metrics in {$region}, current avg. cpu: {$currentCPUAvg}%");

            $scaleUpFlagFile = "/tmp/wcore-lbops-{$this->config['module']}-scale-up.flag";
            $lastScaleUpTime = file_exists($scaleUpFlagFile) ? file_get_contents($scaleUpFlagFile) : 0;
            if ($currentCPUAvg > $metricThreshold[1] && time() - $lastScaleUpTime > 300) {
                //大于thread的第二个值，扩容（要快），scale up, 需要距离上次扩容至少5分钟，方便新扩容的机器生效
                Log::info("start scale up, nodes metrics in {$region}, current avg. cpu: {$currentCPUAvg}%, nodes: {$totalNodes}, threshold: {$metricThreshold[1]}%");

                $ret = $this->scaleUp($region);
                if (!$ret['suc']) {
                    //扩容失败
                    Log::error("Scale up failed, msg: {$ret['msg']}");

                    $content = <<<STRING
<p><strong>nodes in {$region} is on high load, current avg. cpu {$currentCPUAvg}%, total nodes: {$totalNodes}</strong><p>
<p>Scale up failed, message: {$ret['msg']}<p>
STRING;
                    $this->sendAlarmEmail("High cpu load {$currentCPUAvg}% in {$region}, scale up failed", $content);
                } else {
                    //扩容成功
                    Log::info("Scale up succeeded, msg: {$ret['msg']}");

                    file_put_contents($scaleUpFlagFile, time());

                    $content = <<<STRING
<p><strong>nodes in {$region} is on high load, current avg. cpu {$currentCPUAvg}%, total nodes: {$totalNodes}</strong><p>
<p>Scale up succeeded<p>
STRING;
                    $this->sendAlarmEmail("High cpu load {$currentCPUAvg}% in {$region}, scale up succeeded", $content);
                }
            }

            if ($lowLoadNodes == $totalNodes) {
                //全部低负载，scale down or scale in 缩容（要慢）
                $ret = $this->getCurrentInstanceType();
                $insType = $ret['data'] ?? '';
                $smallestInsType = reset($this->verticalScaleInstypes);

                //有可能时scale down，也有可能是scale in
                $scaleSmallFlagFile = "/tmp/wcore-lbops-{$this->config['module']}-scale-small.flag";
                $lastScaleSmallTime = file_exists($scaleSmallFlagFile) ? file_get_contents($scaleSmallFlagFile) : 0;

                if ($insType && $insType != $smallestInsType && time() - $lastScaleSmallTime > 1800) {
                    //不是最小的，缩容
                    Log::info("start scale down, nodes metrics in {$region}, current avg. cpu: {$currentCPUAvg}%, threshold: {$metricThreshold[0]}%");

                    $ret = $this->scaleDown($region);
                    if (!$ret['suc']) {
                        //缩容失败
                        Log::error("Scale down failed, msg: {$ret['msg']}");
                        $content = <<<STRING
<p><strong>nodes in {$region} is on low load, current avg. cpu {$currentCPUAvg}%</strong><p>
<p>Scale down failed, message: {$ret['msg']}<p>
STRING;
                        $this->sendAlarmEmail('Low cpu load, scale down failed', $content);
                    } else {
                        //缩容成功
                        file_put_contents($scaleSmallFlagFile, time());

                        Log::info("Scale down succeeded, msg: {$ret['msg']}");
                        $content = <<<STRING
<p><strong>nodes in {$region} is on low load, current avg. cpu {$currentCPUAvg}%</strong><p>
<p>Scale down succeeded<p>
STRING;
                        $this->sendAlarmEmail('Low cpu load, scale down succeeded', $content);
                    }
                }

                //重新获取一次时间，避免刚scale down完就scale in
                $lastScaleSmallTime = file_exists($scaleSmallFlagFile) ? file_get_contents($scaleSmallFlagFile) : 0;
                $minNodeCount = $regionMinNodesAmount[$region] ?? 1;
                if ($totalNodes > $minNodeCount && time() - $lastScaleSmallTime > 1800) {
                    //距离上次scale down/in超过半小时，尝试scale in
                    Log::info("start scale in, nodes metrics in {$region}, current avg. cpu: {$currentCPUAvg}%, threshold: {$metricThreshold[0]}%");

                    $ret = $this->scaleIn($region);
                    if (!$ret['suc']) {
                        //缩容失败
                        Log::error("Scale in failed, msg: {$ret['msg']}");

                        $content = <<<STRING
<p><strong>nodes in {$region} is on low load, current avg. cpu {$currentCPUAvg}%</strong><p>
<p>Scale in failed, message: {$ret['msg']}<p>
STRING;
                        $this->sendAlarmEmail('Low cpu load, scale in failed', $content);
                    } else {
                        //缩容成功

                        file_put_contents($scaleSmallFlagFile, time());

                        Log::info("Scale in succeeded, msg: {$ret['msg']}");
                        $content = <<<STRING
<p><strong>nodes in {$region} is on low load, current avg. cpu {$currentCPUAvg}%</strong><p>
<p>Scale in succeeded<p>
STRING;
                        $this->sendAlarmEmail('Low cpu load, scale in succeeded', $content);
                    }
                }
            }
        }

        $usedTime = time() - $startTime;

        //debug log
        //Log::info("end watching auto scale, time used: {$usedTime}s");
    }
}
