package com.deepctrls.message.rabbitmq.service.impl;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSONObject;
import com.deepctrls.baFm.model.FmPushDeviceAlarmRequest;
import com.deepctrls.baFm.model.FmRegisterDeviceRequest;
import com.deepctrls.common.DeepCtrlsHttpUtils;
import com.deepctrls.huiyun.constant.HuiYunConstant;
import com.deepctrls.message.enums.CatlFmAlterTypeEnum;
import com.deepctrls.message.enums.CatlFmSiteCodeEnum;
import com.deepctrls.message.rabbitmq.common.RabbitMQConfig;
import com.deepctrls.message.rabbitmq.service.*;
import com.deepctrls.other.model.ProjectAtlSms;
import com.deepctrls.other.service.CompanyDataService;
import com.deepctrls.sight.pushSummary.constant.PushSummaryConstant;
import com.deepctrls.sight.pushSummary.service.PushSummaryService;
import com.deepctrls.sight.runningmsg.constant.RunningMsgConstant;
import com.deepctrls.thinkbos.alarm.constant.AlarmConstant;
import com.deepctrls.thinkbos.alarm.constant.ThinkbosAlarmConstant;
import com.deepctrls.thinkbos.alarm.model.*;
import com.deepctrls.thinkbos.alarm.model.po.AssetRegisterFmPO;
import com.deepctrls.thinkbos.alarm.repository.AlarmSmsConfigRepository;
import com.deepctrls.thinkbos.alarm.repository.AssetRegisterFmRepository;
import com.deepctrls.thinkbos.alarm.service.AlarmLinkageService;
import com.deepctrls.thinkbos.alarm.service.AlarmService;
import com.deepctrls.thinkbos.alarm.service.ThinkbosAlarmConfigService;
import com.deepctrls.thinkbos.authorise.common.util.AuthoriseConstants;
import com.deepctrls.thinkbos.authorise.model.AuthoriseDeepCtrlsUser;
import com.deepctrls.thinkbos.authorise.service.AuthoriseDeepCtrlsUserService;
import com.deepctrls.thinkbos.basicinfo.repository.OrganizationEmployeeRepository;
import com.deepctrls.thinkbos.bos.datamapping.model.DataMapping;
import com.deepctrls.thinkbos.bos.datamapping.service.DataMappingService;
import com.deepctrls.thinkbos.common.util.DateUtil;
import com.deepctrls.thinkbos.common.util.ObjectCopyUtil;
import com.deepctrls.thinkbos.equipment.tEquipment.model.TEquipmentAsset;
import com.deepctrls.thinkbos.equipment.tEquipment.model.TEquipmentClassification;
import com.deepctrls.thinkbos.equipment.tEquipment.model.TEquipmentLocations;
import com.deepctrls.thinkbos.equipment.tEquipment.service.TEquipmentAssetService;
import com.deepctrls.thinkbos.equipment.tEquipment.service.TEquipmentClassificationService;
import com.deepctrls.thinkbos.equipment.tEquipment.service.TEquipmentLocationsService;
import com.deepctrls.thinkbos.redis.client.RedisClient;
import com.deepctrls.thinkbos.redis.common.ProjectParams;
import com.deepctrls.thinkbos.redis.common.RedisConstant;
import com.deepctrls.thinkbos.redis.service.RedisOperateService;
import com.deepctrls.wechatmp.service.WeChatMpService;
import com.rabbitmq.client.Channel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * All rights Reserved, Designed By 深度智控
 * Copyright:    Copyright(C) 2018-2020
 * Company 南京深度智控科技有限公司
 *
 * @author zzx
 * @version 1.0
 * @date 2020/02/28
 * @Description 消费Sight报警消息
 */
@ConditionalOnProperty(value = {"spring.rabbitmq.enabled"}, matchIfMissing = true)
@Component
@RabbitListener(queues = RabbitMQConfig.QUEUE_SIGHT_ALARM)
public class SightAlarmReceiver {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${spring.profiles.active}")
    private String profilesActive;

    /**
     * 是否报警发送博纳瑞通平台
     */
    @Value("${alarm.sry.bnrt.isSend}")
    private boolean isSend;
    /**
     * 报警发送博纳瑞通平台地址
     */
    @Value("${alarm.sry.bnrt.sendBnrtAlarmUrl}")
    private String sendBnrtAlarmUrl;

    /**
     * 迅饶短信机配置
     */
    @Value("${project.sunFullEnabled}")
    private boolean sunFullEnabled;

    @Value("${project.sunFullOPCName}")
    private String sunFullOPCName;

    @Value("${sms.enabled}")
    private Boolean smsEnabled;

    /**
     * 报警对接FM系统是否开启
     * 默认false
     */
    @Value("${catlDockingFm.enable:false}")
    private Boolean catlDockingFmEnable;

    /**
     * 报警对接FM系统,设备注册接口地址
     * 默认：null
     */
    @Value("${catlDockingFm.registerDeviceUrl:}")
    private String catlDockingFmRegisterDeviceUrl;

    /**
     * 报警对接FM系统,推送报警接口地址
     * 默认：null
     */
    @Value("${catlDockingFm.pushDeviceAlarmUrl:}")
    private String catlDockingFmPushDeviceAlarmUrl;

    @Autowired
    private AlarmService alarmService;

    @Autowired
    private DataMappingService dataMappingService;

    @Autowired
    private AlarmLinkageService alarmLinkageService;

    @Autowired
    private ThinkbosAlarmConfigService thinkbosAlarmConfigService;

    @Autowired
    private PushMsgToAppSender pushMsgToAppSender;

    @Autowired
    private PushMsgToWeChatSender pushMsgToWeChatSender;

    @Autowired
    private AlarmToWorkOrder alarmToWorkOrder;

    @Autowired
    private AlarmAndWorkOrderToWsSender alarmToWsSender;

    @Autowired
    private CompanyDataService companyDataService;

    @Autowired
    private DeepSightSmsSender deepSightSmsSender;

    @Autowired
    private TEquipmentLocationsService tEquipmentLocationsService;

    @Autowired
    private RedisOperateService redisOperateService;

    @Autowired
    private TEquipmentAssetService tEquipmentAssetService;

    @Autowired
    private TEquipmentClassificationService tEquipmentClassificationService;

    @Autowired
    private AuthoriseDeepCtrlsUserService authoriseDeepCtrlsUserService;

    @Autowired
    private AlarmSmsConfigRepository alarmSmsConfigRepository;

    @Autowired
    private WeChatMpService weChatMpService;

    @Autowired
    private PushSummaryService pushSummaryService;

    @Autowired
    private OrganizationEmployeeRepository organizationEmployeeRepository;

    @Autowired
    private AssetRegisterFmRepository assetRegisterFmRepository;

    @Autowired
    private RedisClient redisClient;


    @RabbitHandler
    // 表示开启10个线程监听队列，最大为20个线程
    //@RabbitListener(queues = RabbitMQConfig.QUEUE_SIGHT_ALARM, concurrency = "10-20")
    public void process(String queue, Channel channel, Message message) throws IOException {
        try {
            //channel.basicQos(1);

            // 以下是消息格式
            // {"alarmConfigId":"40288209707c0d8901707c0e40060000","tagName":"","isAlarm":true,"alarmDetail":"","alarmTimeLong":11111111,"alarmTimeStr":"2020-02-2815:16:17","assetId":"402880e67043f9ba0170442035c9001d","assetName":"","assetDes":"","alarmInfo":{"workOrder":{"workOrderExecutorId":"","workOrderAcceptor":""},"app":{"appTitle":"","appText":"","personIdList":[]},"note":{"noteTemp":"","personIdList":[]},"voice":""}}

            JSONObject msgObj = (JSONObject) JSONObject.parse(new String(message.getBody()));
            logger.info("##SightAlarmReceiver## message:" + msgObj);

            AlarmData alarmData = saveOrUpdateSightAlarmData(msgObj);
            //告诉服务器收到这条消息 已经被我消费了 可以在队列删掉 这样以后就不会再发了 否则消息服务器以为这条消息没处理掉 后续还会在发
            logger.debug("##SightAlarmReceiver## receiver success,alarm description info: {}", (alarmData != null && StringUtils.isNotBlank(alarmData.getDescription())) ? alarmData.getDescription() : "alarm find is null");
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (IOException e) {
            //丢弃这条消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
            logger.error("##SightAlarmReceiver## receiver fail---{}---", e);

            //丢弃这条消息
            //channel.basicNack(message.getMessageProperties().getDeliveryTag(), false,false);
            //消息的标识，false只确认当前一个消息收到，true确认所有consumer获得的消息
            //channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            //ack返回false，并重新回到队列，api里面解释得很清楚
            //channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            //拒绝消息
            //channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
        } catch (Exception e) {
            //丢弃这条消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
            logger.error("##SightAlarmReceiver## receiver fail---{}---", e);
        }
    }

    // 预警处理逻辑
    private void warningMsgPush(ThinkbosAlarmConfigDto thinkbosAlarmConfigDto, String assetId, String tagName, String tagNameVal, boolean isAlarm) {
        // 先取出Redis中上一次的状态值
        String lastRedisValue = redisOperateService.getWarningPushData(thinkbosAlarmConfigDto.getAlarmName());
        if (lastRedisValue == null || !lastRedisValue.equalsIgnoreCase(String.valueOf(isAlarm))) {
            String warningDesc = "";
            // isAlarm为true表示发生报警预警，false表示报警预警恢复
            if (isAlarm) {
                warningDesc = "，满足预警条件，预警值";
            } else {
                warningDesc = "，预警自动恢复，恢复值";
            }
            TEquipmentAsset asset = null;
            if (StringUtils.isBlank(assetId)) {
                // 增加查询计算点位
                asset = tEquipmentAssetService.findAllSpecByTagValueAndOrgIdAndSiteId(tagName.split(",", -1)[0], thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
            } else {
                asset = tEquipmentAssetService.findByAssetId(assetId);
            }
            // 管理员
            List<String> userIdList = new java.util.ArrayList<>(Collections.singletonList(AuthoriseConstants.ADMIN_USERID));
            // 查询这个设备有几个用户可以看
            List<String> queryUserIdList = organizationEmployeeRepository.findUserIdsByAssetNumNoOrgSiteId(asset.getAssetId());
            if (CollectionUtils.isNotEmpty(userIdList)) {
                userIdList.addAll(queryUserIdList);
            }
            String pushMsgDesc = thinkbosAlarmConfigDto.getAlarmDesCustomTemp() + warningDesc + "为[" + tagNameVal + "]";
            pushSummaryService.savePushSummary(pushMsgDesc, PushSummaryConstant.MSG_7_WARNING, userIdList, null, "");
        } else {
            logger.warn("不做任何处理！");
        }
        // 设置上一次的报警预警状态，根据报警的判断逻辑走，仅在第一次发生和第一次恢复的时候处理
        redisOperateService.setWarningPushData(thinkbosAlarmConfigDto.getAlarmName(), isAlarm);
    }

    /**
     * 触发/恢复 Sight报警
     *
     * @param msgObj 下发消息对象
     * @return
     */
    public AlarmData saveOrUpdateSightAlarmData(JSONObject msgObj) {
        String alarmConfigId = msgObj.getString("alarmConfigId");
        String assetId = msgObj.getString("assetId");
        String tagName = msgObj.getString("tagName");
        // 是否推送 websocket
        Boolean sendAlarmSocketBol = msgObj.getBoolean("sendAlarmSocket");
        boolean sendAlarmSocket = Objects.isNull(sendAlarmSocketBol) || sendAlarmSocketBol;

        logger.debug("tagName----{}----", tagName);
        boolean isAlarm = false;
        try {
            isAlarm = msgObj.getBooleanValue("isAlarm");
        } catch (Exception e) {
            isAlarm = (msgObj.getString("isAlarm") == "PUSHED") ? true : false;
        }
        String oldAlarmDesc = msgObj.getString("alarmDetail");
        String tagNameVal = msgObj.getString("tagNameVal");
        String wxObjectId = ProjectParams.getYamlConfigForWxObjectId();
        try {
            if (StringUtils.isNotBlank(tagNameVal)) {
                String[] vals = tagNameVal.split(",");
                List<String> valList = new LinkedList<>();
                for (int i = 0; i < vals.length; i++) {
                    if (vals[i].equals("undefined") || vals[i].equals("null") || StringUtils.isBlank(vals[i])) {
                        valList.add("");
                    } else {
                        valList.add(RedisConstant.formatValueByDecimal(vals[i], RedisConstant.DEFAULTDECIMALFORMAT6));
                    }
                }
                if (valList.size() > 0) tagNameVal = StringUtils.join(valList, ",");
            }
        } catch (Exception e) {
            logger.error("tagNameVal data validate error ----{}----", e);
        }
        logger.debug("tagNameVal----{}----", tagNameVal);
//        logger.debug("alarmConfigId----{}----", alarmConfigId);
//        logger.debug("assetId----{}----", assetId);
//        logger.debug("isAlarm----{}----", isAlarm);
        logger.debug("oldAlarmDesc----{}----", oldAlarmDesc);
//        logger.debug("alarmTimeStr----{}----", alarmTimeStr);

        AlarmData alarmData = new AlarmData();
        try {
            // key：dataVal  value: dataText
            Map<String, Object> alarmLevelMap = thinkbosAlarmConfigService.queryThinkbosAlarmConfigLevelValAndText();

            ThinkbosAlarmConfigDto thinkbosAlarmConfigDto = thinkbosAlarmConfigService.findById(alarmConfigId);

            if (thinkbosAlarmConfigDto != null) {
                logger.debug("--------thinkbosAlarmConfigDto != null--------{}", thinkbosAlarmConfigDto.getId());
                // 预警处理逻辑
                if (thinkbosAlarmConfigDto.getRuleType() != null && ThinkbosAlarmConstant.ALARM_RULE_TYPE_WARNING.equalsIgnoreCase(thinkbosAlarmConfigDto.getRuleType())) {

                    warningMsgPush(thinkbosAlarmConfigDto, assetId, tagName, tagNameVal, isAlarm);

                } else {

                    // 正常报警处理逻辑
                    logger.debug("----saveOrUpdateSightAlarmData----alarmName----[" + thinkbosAlarmConfigDto.getAlarmName() + "]-----[" + isAlarm + "]");
                    // Sight报警使用主键id当作tagname
                    alarmData.setTagname(thinkbosAlarmConfigDto.getAlarmName());

                    // 设置报警点关联的视图信息
                    alarmData.setViewRelationStatus(thinkbosAlarmConfigDto.getViewRelationStatus());
                    alarmData.setViewRelation(thinkbosAlarmConfigDto.getViewRelation());

                    // 根据tagname查询是否有未关闭的报警
                    List<AlarmData> alarmDataList = alarmService.findUnReponseAlarmDataByTagNameAndOrgIdAndSiteId(thinkbosAlarmConfigDto.getAlarmName(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                    // 如果存在有未关闭的报警，并且又再次报警的时候，此时不需要做处理
                    if (!(alarmDataList.size() > 0 && isAlarm)) {

                        // 电梯模板的报警，冷机报警模板的报警，需要额外组装报警描述
                        if (ThinkbosAlarmConstant.ALARM_EMS_TEMPLATE.equalsIgnoreCase(thinkbosAlarmConfigDto.getAlarmDesId()) ||
                                ThinkbosAlarmConstant.ALARM_CH_TEMPLATE.equalsIgnoreCase(thinkbosAlarmConfigDto.getAlarmDesId())) {
                            // 根据描述id，查询短信格式，报警描述内容需要从字典表查询
                            List<DataMapping> alarmDesMappings = dataMappingService.findByIdAndDataType(thinkbosAlarmConfigDto.getAlarmDesId(), ThinkbosAlarmConstant.ALARM_DES_TEMP, thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                            oldAlarmDesc = (alarmDesMappings.size() > 0) ? alarmDesMappings.get(0).getDataText() : oldAlarmDesc;
                        }

                        String alarmDesc = oldAlarmDesc;

                        // 报警类型需要从字典表查询好
                        List<DataMapping> mappings = dataMappingService.findByIdAndDataType(thinkbosAlarmConfigDto.getAlarmTypeId(), ThinkbosAlarmConstant.ALARM_TYPE, thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                        String alarmType = mappings.size() > 0 ? mappings.get(0).getDataText() : "";
                        alarmData.setAlarmtype(alarmType);

                        // 替换报警描述中的报警类型
                        if (StringUtils.isNotBlank(alarmType)) {
                            alarmDesc = alarmDesc.replace("<报警类型>", alarmType);
                        }

                        logger.debug("----saveOrUpdateSightAlarmData----orgId----[{}]----siteId----[{}]----alarmType----[{}]", thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId(), alarmType);

                        // 当前报警时间
//                    LocalDateTime alarmLocalDate = LocalDateTime.parse(alarmTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
//                    Date alarmDate = Date.from(alarmLocalDate.atZone(ZoneId.systemDefault()).toInstant());
                        Date alarmDate = new Date();

                        // isAlarm: 报警状态(true-第一次达到报警条件 false-达到报警条件后第一次脱离报警条件。)
                        // isAlarm为false表示报警信息已经从报警状态恢复成非报警状态了，此时需要更新报警的持续时间
                        logger.debug("----saveOrUpdateSightAlarmData----isAlarm----[{}]----", isAlarm);

                        //根据tagname查询AlarmData
                        AlarmData ad = alarmService.findAlarmDataByTagNameAndOrgIdAndSiteId(thinkbosAlarmConfigDto.getAlarmName(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                        // isAlarm为true，并且点位历史记录的的response字段为1，并且关闭时间有值，并且恢复点位和恢复值都是空的，说明此记录已经被人工在界面上关闭掉了，Redis的缓存数据被清理掉就会出现这种情况，这种情况报警，不做任何处理
                        if (ad != null && isAlarm && ad.getResponse() == 1L && ad.getAlarmCloseTime() != null && StringUtils.isBlank(ad.getRecoveryTag()) && StringUtils.isBlank(ad.getRecoveryVal())) {
                            logger.warn("----isAlarm为true，并且点位历史记录的的response字段为1，并且关闭时间有值，并且恢复点位和恢复值都是空的，说明此记录已经被人工在界面上关闭掉了，Redis的缓存数据被清理掉就会出现这种情况，这种情况报警，不做任何处理----");
                        }
                        // Response为2表示状态处于处理中，此时处理中状态不做任何处理
                        else if (ad != null && "2".equals(ad.getResponse().toString())) {
                            logger.warn("----saveOrUpdateSightAlarmData----Response为2表示状态处于处理中，此时处理中状态不做任何处理----[{}]----", ad.getResponse());
                        } else {
                            // 根据tagname找到对应的设备信息
                            TEquipmentAsset asset = null;
                            try {
                                if (StringUtils.isBlank(assetId)) {
                                    // 增加查询计算点位
                                    //asset = tEquipmentAssetService.findByTagValueAndOrgIdAndSiteId(tagName.split(",", -1)[0], thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                    asset = tEquipmentAssetService.findAllSpecByTagValueAndOrgIdAndSiteId(tagName.split(",", -1)[0], thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                } else {
                                    asset = tEquipmentAssetService.findByAssetId(assetId);
                                }
                                if (asset != null) {
                                    alarmData.setAssetname(asset.getDescription()); // 存放设备名称描述
                                    alarmData.setAssetnum(asset.getAssetId());
                                    // 替换报警描述中的设备名称
                                    if (StringUtils.isNotBlank(asset.getDescription())) {
                                        alarmDesc = alarmDesc.replace("<设备名称>", asset.getDescription());
                                    }

                                    // 根据子系统编码获取子系统信息
                                    TEquipmentClassification tEquipmentClassification = tEquipmentClassificationService.findBySubsystemAndOrgIdAndSiteId(thinkbosAlarmConfigDto.getSubsystem(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                    String systemId = tEquipmentClassification != null ? tEquipmentClassification.getClassificationId() : "";
                                    String systemName = tEquipmentClassification != null ? tEquipmentClassification.getDescription() : "";
                                    if (StringUtils.isBlank(systemId)) {
                                        TEquipmentClassification classification = tEquipmentClassificationService.findBySubsystemAndOrgIdAndSiteId(asset.getSubsystem(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                        systemId = classification != null ? classification.getClassificationId() : alarmData.getAssetnum().substring(0, 5) + "000000";
                                        systemName = classification != null ? classification.getDescription() : thinkbosAlarmConfigDto.getSubsystem();
                                    }
                                    alarmData.setSystem(systemName);

                                    // tagName前缀
                                    String tagNamePrefixStr = thinkbosAlarmConfigDto.getTagName().substring(0, thinkbosAlarmConfigDto.getTagName().lastIndexOf("#") + 1);

                                    // 电梯模板的报警，需要组装电梯的故障代码和当前电梯楼层
                                    if (ThinkbosAlarmConstant.ALARM_EMS_TEMPLATE.equalsIgnoreCase(thinkbosAlarmConfigDto.getAlarmDesId())) {
                                        String dtFaultCode = tagNamePrefixStr + "fault_code";
                                        String dtFaultValue = redisOperateService.getCacheDataByTagNameAndValue(dtFaultCode, redisOperateService.getAcqdataRData(dtFaultCode));
                                        logger.debug("组装电梯的故障代码dt----dtFaultCode----{}----dtFaultValue----{}----", dtFaultCode, dtFaultValue);
                                        if (StringUtils.isNotBlank(dtFaultValue)) {
                                            alarmDesc = alarmDesc.replace("<故障码翻译>", dtFaultValue);
                                        }
                                        // 组装当电梯楼层
                                        dtFaultCode = tagNamePrefixStr + "pos";
                                        dtFaultValue = redisOperateService.getAcqdataRData(dtFaultCode);
                                        logger.debug("组装当电梯楼层dt----dtFaultCode----{}----dtFaultValue----{}----", dtFaultCode, dtFaultValue);
                                        if (StringUtils.isNotBlank(dtFaultValue)) {
                                            alarmDesc = alarmDesc.replace("<当前楼层>", dtFaultValue);
                                        }
                                    }

                                    // 冷机报警模板的报警，需要组装冷机故障代码
                                    if (ThinkbosAlarmConstant.ALARM_CH_TEMPLATE.equalsIgnoreCase(thinkbosAlarmConfigDto.getAlarmDesId())) {
                                        String dtFaultCode = tagNamePrefixStr + "fault_code";
                                        String dtFaultValue = redisOperateService.getCacheDataByTagNameAndValue(dtFaultCode, redisOperateService.getAcqdataRData(dtFaultCode));
                                        logger.debug("组装冷机的故障代码lj----dtFaultCode----{}----dtFaultValue----{}----", dtFaultCode, dtFaultValue);
                                        if (StringUtils.isNotBlank(dtFaultValue)) {
                                            alarmDesc = alarmDesc.replace("<冷机故障代码翻译>", dtFaultValue);
                                        }
                                    }

                                    // 查询报警设备的位置
                                    try {
                                        TEquipmentLocations tEquipmentLocations = tEquipmentLocationsService.findByLocationsId(asset.getLocationsId());
                                        alarmData.setLocation(tEquipmentLocations != null ? tEquipmentLocations.getDescription() : "");
                                    } catch (Exception e) {
                                        alarmData.setLocation("");
                                    }
                                    // 替换报警描述中的设备位置
                                    alarmDesc = alarmDesc.replace("<设备位置>", alarmData.getLocation());
                                    // 替换报警描述中的报警值
                                    alarmDesc = alarmDesc.replace("<报警值>", "报警值：" + tagNameVal);

                                    alarmData.setDescription(alarmDesc);
                                    alarmData.setComment(alarmDesc);

                                    // isAlarm为true表示发生报警，false表示报警恢复
                                    if (!isAlarm) {
                                        logger.warn("----saveOrUpdateSightAlarmData----false表示报警恢复----[{}]----", isAlarm);
                                        // 更新当前报警点位的持续时间操作
                                        // if (ad != null && ad.getResponse() != AlarmConstant.TREATED) {
                                        // 人工关闭，等自动恢复后，也要更新对应的恢复值
                                        /*if(StrUtil.equalsIgnoreCase(tagName,"CATL#HXBA#HVC#H2_DHU_105#WORKSHOP_T")){
                                            logger.debug("--------消费了---CATL#HXBA#HVC#H2_DHU_105#WORKSHOP_T");
                                        }*/
                                        if (ad != null && (ad.getResponse() != AlarmConstant.TREATED || Objects.nonNull(ad.getAlarmCloseTime()))) {
                                            try {
                                                long continuesecond = (alarmDate.getTime() - ad.getTime().getTime()) / 1000;
                                                // 人工关闭后自动恢复，不要更新 continuesecond、status等字段
                                                if (Objects.isNull(ad.getAlarmCloseTime())) {
                                                    // 更新实时报警表中的持续时间和报警状态（报警状态为1表示实时报警是否一闪而过（1一闪而过，否则不是））
                                                    ad.setContinuesecond(continuesecond);
                                                    ad.setStatustime(alarmDate);
                                                    ad.setStatus(AlarmConstant.AUTORECOVER);
                                                    ad.setResponsetime(alarmDate);
                                                }
                                                ad.setResponse(AlarmConstant.TREATED);

                                                //新增报警时 报警信息有可能改变  每次新增报警数据时都更新以下字段
                                                ad.setAlarmtype(alarmData.getAlarmtype());
                                                ad.setAssetname(alarmData.getAssetname());
                                                ad.setAssetnum(alarmData.getAssetnum());
                                                // 2019.07.19约定，报警恢复的时候不更新报警描述
//                                            ad.setComment(alarmData.getComment());
//                                            ad.setDescription(alarmData.getDescription());
                                                ad.setFloor(alarmData.getFloor());
                                                ad.setSystem(alarmData.getSystem());
                                                ad.setLocation(alarmData.getLocation());

                                                // 更新关联视图信息
                                                ad.setViewRelation(alarmData.getViewRelation());
                                                ad.setViewRelationStatus(alarmData.getViewRelationStatus());

                                                // 更新自动恢复的报警到为已处理，只针对慧云使用
                                                ad.setDealStatus(HuiYunConstant.HUIYUN_DEALSTATUS_2);
                                                // 存放报警恢复时的tagname和报警值
                                                ad.setRecoveryTag(tagName);
                                                ad.setRecoveryVal(tagNameVal);

                                                //判断报警是否屏蔽
                                                ad = modifyAdRestrain(ad, false);

                                                alarmData = alarmService.saveAlarmData(ad);

                                                if (Objects.isNull(ad.getAlarmCloseTime())) {
                                                    // 同步更新历史报警表中的持续时间和报警状态  20211103 增加一个报警屏蔽状态也更新到历史表
                                                    alarmService.updateHistoryAlarmContinuetimeAndRecoveryVal(continuesecond, alarmDate, ad.getId(), tagName, tagNameVal, ad.getRestrain());
                                                } else {
                                                    // 更新 statustime的原因，方便查询那些人工关闭恢复时间，但不更新 status因为前端后后台改动较多，及时不改也不会影响逻辑
                                                    alarmService.updateHistoryRecoveryForArtificialClose(alarmDate, ad.getId(), tagName, tagNameVal, ad.getRestrain());
                                                }
                                                logger.warn("----saveOrUpdateSightAlarmData----更新实时报警表和历史报警表中的持续时间、报警状态、报警恢复值----");

                                                // 报警恢复，发送报警信号给到web端的websocket通知到前台进行刷新
                                                try {
                                                    AlarmConfigAndAlarmPopupStatusDto alarmConfigAndAlarmPopupStatusDto = ObjectCopyUtil.copyProperties(alarmData, AlarmConfigAndAlarmPopupStatusDto.class);
                                                    // 是否推送到ws ； ps：清空报警redis发送状态时，下一次不要推送，不然列表不停的刷新
                                                    if (sendAlarmSocket) {
                                                        alarmToWsSender.sendAlarmToWs(alarmConfigAndAlarmPopupStatusDto);
                                                    }
                                                } catch (Exception e) {
                                                    logger.error("alarm send websocket error ----{}----", e);
                                                }

                                                // 更新联动对应的状态
                                                alarmLinkageService.updateLinkageHistoryStatus(thinkbosAlarmConfigDto.getAlarmName(), ad.getTime(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());

                                                // 判断报警是否推送到博纳瑞通的平台
                                                try {
                                                    if (isSend) {
                                                        String sendAlarmStr = "设备[" + alarmData.getAssetname() + "]的报警已恢复";
                                                        logger.debug("----alarm send to bnrt platform----恢复----" + sendAlarmStr);
                                                        sendAlarmDataToBnrtPlatform("恢复", alarmData.getAlarmtype(), sendAlarmStr);
                                                    }
                                                } catch (Exception e) {
                                                    logger.error("alarm send to bnrt platform error ----{}----", e);
                                                }

                                            } catch (Exception e) {
                                                logger.error("----saveOrUpdateSightAlarmData----更新报警点[{}]持续时间的时候，转换时间报错了--------", ad.getTagname());
                                            }
                                        } else {
                                            logger.warn("----报警恢复，但是已被人工在界面上处理掉了，不做任何处理----");
                                        }
                                    } else {
                                        logger.warn("----saveOrUpdateSightAlarmData----isAlarm==true----[{}]----", isAlarm);
                                        // Sight报警，默认级别为低，报警级别需要从字典表查询
                                        try {
                                            if (StringUtils.isNotBlank(thinkbosAlarmConfigDto.getAlarmLevel())) {
                                                List<DataMapping> alarmLevels = dataMappingService.findByIdAndDataType(thinkbosAlarmConfigDto.getAlarmLevel(), ThinkbosAlarmConstant.ALARM_LEVEL, thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                                alarmData.setPriority(Long.parseLong(alarmLevels.size() > 0 ? alarmLevels.get(0).getDataVal() : String.valueOf(AlarmConstant.LOW)));
                                            } else {
                                                alarmData.setPriority(AlarmConstant.LOW);
                                            }
                                        } catch (NumberFormatException e) {
                                            alarmData.setPriority(AlarmConstant.LOW);
                                        }

                                        // 计算报警点位今日累计次数、当月累计次数和历史累计次数
                                        long daySum = 0;
                                        long monthSum = 0;
                                        long totalSum = 0;
                                        try {
                                            daySum = alarmService.getHistoryCountByTagName(thinkbosAlarmConfigDto.getAlarmName(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId(), getCurrentDate(null) + " 00:00:00");
                                            monthSum = alarmService.getHistoryCountByTagName(thinkbosAlarmConfigDto.getAlarmName(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId(), DateUtil.format(alarmDate, DateUtil.yyyyMM) + "-01 00:00:00");
                                            totalSum = alarmService.getHistoryCountByTagName(thinkbosAlarmConfigDto.getAlarmName(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId(), null);
                                        } catch (Exception e) {
                                            logger.error("----saveOrUpdateSightAlarmData----计算报警点位今日累计次数和历史累计次数error----[{}]----", e);
                                        }
                                        alarmData.setTime(alarmDate);

                                        // ------------------------视频联动配置逻辑处理------------------------
                                        // 判断报警点位是否关联了视频配置，如果存在视频配置需要记录视频联动数据
                                        Map<String, List<ThinkbosAlarmConfigNote>> alarmLinkageConfigMap = thinkbosAlarmConfigService.findItemByAlarmName(thinkbosAlarmConfigDto.getAlarmName());
                                        // 记录配置的视频联动token
                                        String videoLinkageToken = "";
                                        if (alarmLinkageConfigMap.get(ThinkbosAlarmConstant.VEDIO) != null) {
                                            List<ThinkbosAlarmConfigNote> videoConfigs = alarmLinkageConfigMap.get(ThinkbosAlarmConstant.VEDIO);
                                            if (videoConfigs.size() > 0) {
                                                List<String> assetIds = new ArrayList<>();
                                                videoConfigs.forEach(videoConfig -> {
                                                    assetIds.add(videoConfig.getReceiverId());
                                                });
                                                List<TEquipmentAsset> tEquipmentAssets = tEquipmentAssetService.findByAssetIdInAndOrgIdAndSiteId(assetIds, thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                                try {
                                                    videoLinkageToken = tEquipmentAssets.size() > 0 ? tEquipmentAssets.get(0).getAssetName() : "";
                                                    List<AlarmLinkageHistory> histories = new ArrayList<>();
                                                    tEquipmentAssets.forEach(tEquipmentAsset -> {
                                                        AlarmLinkageHistory history = new AlarmLinkageHistory();
                                                        history.setLinkageTime(alarmDate);
                                                        history.setLinkageType(AlarmConstant.LINKAGETYPE_VIDEO);
                                                        history.setLinkageValue(tEquipmentAsset.getAssetName());
                                                        history.setStatus(AlarmConstant.LINKAGESTATUS_R);
                                                        history.setTagName(thinkbosAlarmConfigDto.getAlarmName());
                                                        history.setOrgId(thinkbosAlarmConfigDto.getOrgId());
                                                        history.setSiteId(thinkbosAlarmConfigDto.getSiteId());
                                                        histories.add(history);
                                                    });
                                                    alarmLinkageService.saveAllHistory(histories);
                                                } catch (Exception e) {
                                                    logger.warn("----视频配置的设备非标准的视频名称----thinkbosAlarmConfigDto.getAlarmName()----{}", thinkbosAlarmConfigDto.getAlarmName());
                                                }
                                            }
                                        }

                                        // 根据点位tagname判断是否已经存在在现在的报警记录表中，
                                        // 如果不存在，先在alarm_alarm_data表中插入一条记录，再同步在alarm_alarm_history表中插入一条记录，
                                        // 如果存在的话就先更新alarm_alarm_data表中的数据，再在历史记录表中增加一条记录

                                        // if中是存在的情况
                                        if (ad != null) {

                                            // 更新alarm_alarm_data表中的数据
                                            ad.setTime(alarmDate);
                                            ad.setResponse((long) 0);
                                            ad.setResponsetime(null);
                                            ad.setNotation("");
                                            ad.setStatus((long) 0);
                                            ad.setStatustime(null);
                                            ad.setDaynum(daySum); // 记录当天该报警点报警次数
                                            ad.setMonthnum(monthSum); // 记录当月该报警点报警次数
                                            ad.setTotalnum(totalSum); // 记录该报警点累计报警次数
                                            ad.setSystem(alarmData.getSystem());
                                            ad.setStartdealtime(null);
                                            ad.setContinuesecond(null);

                                            // 存放报警发生的实际的tagname和报警值
                                            ad.setAlarmTag(tagName);
                                            ad.setAlarmVal(tagNameVal);
                                            // 存放报警恢复时的tagname和报警值
                                            ad.setRecoveryTag(null);
                                            ad.setRecoveryVal(null);
                                            // 新产生的报警需要更新报警记录为未处理，只针对慧云使用
                                            ad.setDealStatus(HuiYunConstant.HUIYUN_DEALSTATUS_0);

                                            //新增报警时 报警信息有可能改变  每次新增报警数据时都更新以下字段
                                            ad.setAlarmtype(alarmData.getAlarmtype());
                                            ad.setAssetname(alarmData.getAssetname());
                                            ad.setAssetnum(alarmData.getAssetnum());
                                            ad.setComment(alarmData.getComment());
                                            ad.setDescription(alarmData.getDescription());
                                            ad.setFloor(alarmData.getFloor());
                                            ad.setGroupid(alarmData.getGroupid());
                                            ad.setGroupname(alarmData.getGroupname());
                                            ad.setLocation(alarmData.getLocation());
                                            ad.setPriority(alarmData.getPriority());
                                            ad.setEx1(StringUtils.isNotBlank(videoLinkageToken) ? videoLinkageToken : "");// 2019/03/09 开始用做报警联动标记字段，[存放视频对应的token]
                                            ad.setEx2(StringUtils.isNotBlank(videoLinkageToken) ? AlarmConstant.LINKAGE_VIDEO_TAG : "");// 2019/03/09 开始用做报警联动标记字段，[video表示视频联动]
                                            ad.setWorkOrderId(null);
                                            ad.setWorkOrderNum(null);
                                            ad.setDealmethod("");
                                            ad.setDealperson("");
                                            ad.setDealpersonid("");
                                            ad.setDealsecond(null);
                                            ad.setAlarmreason("");
                                            //判断报警是否屏蔽，若恢复成了未屏蔽，同步更新到报警历史表
                                            ad = modifyAdRestrain(ad, true);

                                            // 触发报警时，清空 处理时间、关闭人ID、关闭人名称、关闭时间、关闭描述 add by jingmh
                                            ad.setDealTime(null);
                                            ad.setAlarmCloseUserId(null);
                                            ad.setAlarmCloseUserName(null);
                                            ad.setAlarmCloseTime(null);
                                            ad.setAlarmCloseRemark(null);

                                            alarmData = alarmService.saveAlarmData(ad);
                                            logger.debug("----saveOrUpdateSightAlarmData----ad != null----保存了实时报警记录----");

                                        } else {
                                            // 此处为在实时报警中不存在的情况
                                            alarmData.setDaynum(daySum); // 记录当天该报警点报警次数
                                            alarmData.setMonthnum(monthSum); // 记录当月该报警点报警次数
                                            alarmData.setTotalnum(totalSum); // 记录该报警点累计报警次数
                                            alarmData.setSiteid(thinkbosAlarmConfigDto.getSiteId());
                                            alarmData.setOrgid(thinkbosAlarmConfigDto.getOrgId());
                                            alarmData.setContinuesecond(null);
                                            alarmData.setResponse((long) 0);
                                            alarmData.setRestrain((long) 0);
                                            alarmData.setStatus((long) 0);
                                            alarmData.setEx1(StringUtils.isNotBlank(videoLinkageToken) ? videoLinkageToken : "");// 2019/03/09 开始用做报警联动标记字段，[存放视频对应的token]
                                            alarmData.setEx2(StringUtils.isNotBlank(videoLinkageToken) ? AlarmConstant.LINKAGE_VIDEO_TAG : "");// 2019/03/09 开始用做报警联动标记字段，[video表示视频联动]
                                            alarmData.setWorkOrderId(null);
                                            alarmData.setWorkOrderNum(null);
                                            alarmData.setDealmethod("");
                                            alarmData.setDealperson("");
                                            alarmData.setDealpersonid("");
                                            alarmData.setDealsecond(null);
                                            alarmData.setAlarmreason("");
                                            // 新产生的报警需要更新报警记录为未处理，只针对慧云使用
                                            alarmData.setDealStatus(HuiYunConstant.HUIYUN_DEALSTATUS_0);
                                            // 存放报警发生的实际的tagname和报警值
                                            alarmData.setAlarmTag(tagName);
                                            alarmData.setAlarmVal(tagNameVal);
                                            // 存放报警恢复时的tagname和报警值
                                            alarmData.setRecoveryTag(null);
                                            alarmData.setRecoveryVal(null);
                                            alarmData = alarmService.saveAlarmData(alarmData);

                                            logger.warn("----saveOrUpdateSightAlarmData----此处为在实时报警中不存在的情况----保存了实时报警记录----");
                                        }
                                        // 先更新历史报警表中的dataid为空，保证新增加的历史报警的dataid唯一
                                        alarmService.updateDataIdToNull(alarmData.getId());
                                        logger.warn("----saveOrUpdateSightAlarmData----更新历史报警表中的dataid为空----");


                                        // 同时保存历史报警信息
                                        AlarmHistory alarmHistory = new AlarmHistory(alarmData.getTime(), alarmData.getTagname(), alarmData.getResponse(), alarmData.getResponsetime(), alarmData.getRestrain(), alarmData.getRestraintime(), alarmData.getRestrainNum(), alarmData.getRestrainEndTime(), alarmData.getContinuesecond(), alarmData.getNotation(), alarmData.getStatus(), alarmData.getStatustime(), alarmData.getDealpersonid(), alarmData.getDealperson(), alarmData.getDaynum(), alarmData.getTotalnum(), alarmData.getSiteid(), alarmData.getOrgid(), alarmData.getId());
                                        alarmHistory.setPriority(alarmData.getPriority());
                                        alarmHistory.setAlarmtype(alarmData.getAlarmtype());
                                        alarmHistory.setSystem(alarmData.getSystem());
                                        alarmHistory.setMonthnum(monthSum); // 记录当月该报警点报警次数
                                        alarmHistory.setAssetId(alarmData.getAssetnum()); // 设备id
                                        // 新产生的报警需要更新报警记录为未处理，只针对慧云使用
                                        alarmHistory.setDealStatus(HuiYunConstant.HUIYUN_DEALSTATUS_0);
                                        // 存放报警发生的实际的tagname和报警值
                                        alarmHistory.setAlarmTag(tagName);
                                        alarmHistory.setAlarmVal(tagNameVal);
                                        alarmService.saveAlarmHistoryData(alarmHistory);
                                        logger.debug("----saveOrUpdateSightAlarmData----保存了历史报警数据----");


                                   /* String priorityName = "普通";
                                    if (alarmData.getPriority().equals(AlarmConstant.HIGH)) {
                                        priorityName = "高级";
                                    } else if (alarmData.getPriority().equals(AlarmConstant.MIDDLE)) {
                                        priorityName = "中级";
                                    }*/
                                        // 报警等级重表里读取
                                        String priorityName = "";
                                        if (MapUtil.isNotEmpty(alarmLevelMap) && Objects.nonNull(alarmData.getPriority())) {
                                            Object o = alarmLevelMap.get(alarmData.getPriority().toString());
                                            if (Objects.nonNull(o)) {
                                                priorityName = o.toString();
                                            }
                                        }


                                        //判断报警是否屏蔽中，若为屏蔽中，不推送前端消息
                                        if (judgeAdRestrain(alarmData.getRestrain(), alarmData.getRestrainEndTime())) {
                                            // 发送报警信号给到web端的websocket通知到前台进行刷新
                                            try {
                                                AlarmConfigAndAlarmPopupStatusDto alarmConfigAndAlarmPopupStatusDto = ObjectCopyUtil.copyProperties(alarmData, AlarmConfigAndAlarmPopupStatusDto.class);
                                                alarmConfigAndAlarmPopupStatusDto.setAlarmPopupStatus(thinkbosAlarmConfigDto.getAlarmPopupStatus());
                                                alarmConfigAndAlarmPopupStatusDto.setVoiceRelation(thinkbosAlarmConfigDto.getVoiceRelation());

                                                // 是否推送到ws ； ps：清空报警redis发送状态时，下一次不要推送，不然列表不停的刷新
                                                if (sendAlarmSocket) {
                                                    alarmToWsSender.sendAlarmToWs(alarmConfigAndAlarmPopupStatusDto);
                                                }

                                            } catch (Exception e) {
                                                logger.error("alarm send websocket error ----{}----", e);
                                            }

                                            // 判断报警是否需要触发短信
                                            if (smsEnabled) {
                                                try {
                                                    String sendMsg = ""; // 短信发送内容
                                                    // 根据短信模板id，查询短信格式，报警短信内容需要从字典表查询
                                                    List<DataMapping> noteDataMappings = dataMappingService.findByIdAndDataType(thinkbosAlarmConfigDto.getNoteTempId(), ThinkbosAlarmConstant.ALARM_NOTE_TEMP, thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                                    if (noteDataMappings.size() > 0) {
                                                        sendMsg = noteDataMappings.get(0).getDataText();
                                                        sendMsg = sendMsg.replaceAll("<级别>", priorityName);
                                                        sendMsg = sendMsg.replaceAll("<设备名称>", alarmData.getAssetname());
                                                        sendMsg = sendMsg.replaceAll("<报警描述>", alarmData.getDescription());
                                                        sendMsg = sendMsg.replaceAll("<报警类型>", alarmType);
                                                        sendMsg = sendMsg.replaceAll("<时:分>", DateUtil.format(new Date(), DateUtil.HHmm));
                                                        sendMsg = sendMsg.replaceAll("<报警值>", "报警值：" + tagNameVal);
                                                        sendMsg = sendMsg.replaceAll("<累计次数>", "累计" + alarmData.getTotalnum() + "次");
                                                        // 判断是否有<故障码翻译>
                                                        if (sendMsg.indexOf("<故障码翻译>") != -1) {
                                                            // 电梯模板的报警，需要组装电梯的故障代码和当前电梯楼层
                                                            String dtFaultCode = tagNamePrefixStr + "fault_code";
                                                            String dtFaultValue = redisOperateService.getCacheDataByTagNameAndValue(dtFaultCode, redisOperateService.getAcqdataRData(dtFaultCode));
                                                            logger.debug("sms----组装电梯的故障代码dt----dtFaultCode----{}----dtFaultValue----{}----", dtFaultCode, dtFaultValue);
                                                            if (StringUtils.isNotBlank(dtFaultValue)) {
                                                                sendMsg = sendMsg.replaceAll("<故障码翻译>", dtFaultValue);
                                                            }
                                                        }
                                                        if (sendMsg.indexOf("<当前楼层>") != -1) {
                                                            // 组装当电梯楼层
                                                            String dtFaultCode = tagNamePrefixStr + "pos";
                                                            String dtFaultValue = redisOperateService.getAcqdataRData(dtFaultCode);
                                                            logger.debug("sms----组装当电梯楼层dt----dtFaultCode----{}----dtFaultValue----{}----", dtFaultCode, dtFaultValue);
                                                            if (StringUtils.isNotBlank(dtFaultValue)) {
                                                                sendMsg = sendMsg.replaceAll("<当前楼层>", dtFaultValue);
                                                            }
                                                        }
                                                        if (sendMsg.indexOf("<冷机故障代码翻译>") != -1) {
                                                            // 暖通空调系统报警，需要组装冷机的故障代码
                                                            String dtFaultCode = tagNamePrefixStr + "fault_code";
                                                            String dtFaultValue = redisOperateService.getCacheDataByTagNameAndValue(dtFaultCode, redisOperateService.getAcqdataRData(dtFaultCode));
                                                            logger.debug("sms----组装冷机的故障代码lj----dtFaultCode----{}----dtFaultValue----{}----", dtFaultCode, dtFaultValue);
                                                            if (StringUtils.isNotBlank(dtFaultValue)) {
                                                                sendMsg = sendMsg.replaceAll("<冷机故障代码翻译>", dtFaultValue);
                                                            }
                                                        }
                                                    } else {
                                                        // 没查到短信模板使用默认标准模板，模板内容 : <级别>报警：<设备名称>发生报警，<报警描述>
                                                        sendMsg = priorityName + "报警：" + alarmData.getAssetname() + "发生报警，" + oldAlarmDesc;
                                                    }

                                                    // 优先判断是否需要给迅饶短信及发送短信
                                                    if (sunFullEnabled) {
                                                        ProjectAtlSms projectAtlSms = companyDataService.findByAlarmName(thinkbosAlarmConfigDto.getAlarmName());
                                                        if (projectAtlSms != null) {
                                                            logger.debug("sunfullSetValue，opcName：[{}]，sendMsg： [{}]", projectAtlSms.getOpcName(), sendMsg);
                                                            companyDataService.sunfullSetValue(projectAtlSms.getOpcName(), sendMsg);
                                                        } else {
                                                            logger.warn("sunfull 报警点位[{}]发生了报警，未配置相应的短信接收人!", thinkbosAlarmConfigDto.getAlarmName());
                                                        }
                                                    } else if (alarmLinkageConfigMap.get(ThinkbosAlarmConstant.NOTE) != null) {
                                                        List<ThinkbosAlarmConfigNote> smsConfigs = alarmLinkageConfigMap.get(ThinkbosAlarmConstant.NOTE);
                                                        if (smsConfigs.size() > 0) {
                                                            List<String> userIds = new ArrayList<>();
                                                            smsConfigs.forEach(smsConfig -> {
                                                                userIds.add(smsConfig.getReceiverId());
                                                            });

                                                            // 查询短信配置对应的人员电话信息
                                                            List<AuthoriseDeepCtrlsUser> users = authoriseDeepCtrlsUserService.findAuthoriseDeepCtrlsUserByIds(userIds);
                                                            StringBuffer smsReceiverBuffer = new StringBuffer("");

                                                            users.stream().filter(item -> StringUtils.isNotBlank(item.getMobile())).forEach(authoriseDeepCtrlsUser -> {
                                                                smsReceiverBuffer.append("+86");
                                                                smsReceiverBuffer.append(authoriseDeepCtrlsUser.getMobile());
                                                                smsReceiverBuffer.append(",");
                                                            });

                                                            if (smsReceiverBuffer.toString().endsWith(",")) {
                                                                String smsReceiver = StringUtils.substringBeforeLast(smsReceiverBuffer.toString(), ",");
                                                                logger.debug("----send sms to receiver----phone:{}----sendMsg----{}", smsReceiver, sendMsg);
                                                                // 发送短信到消息队列
                                                                deepSightSmsSender.sendDeepSightSmsToMQ(smsReceiver, sendMsg);
                                                            } else {
                                                                logger.warn("报警点位[{}]发生了报警，未配置相应的短信接收人!", thinkbosAlarmConfigDto.getAlarmName());
                                                            }
                                                        }
                                                    } else {
                                                        logger.warn("已匹配完成所有规则，报警点位[{}]未配置相应的短信接收人!", thinkbosAlarmConfigDto.getAlarmName());
                                                    }
                                                } catch (Exception e) {
                                                    logger.error("alarm send sms error ----{}----", e);
                                                }
                                            } else {
                                                logger.info("报警功能未配置允许发送短信功能!");
                                            }

                                            // 判断报警是否需要触发工单
                                            try {
                                                JSONObject jsonObject = new JSONObject();
                                                jsonObject.put("orderDes", alarmData.getDescription()); // 工单描述
                                                jsonObject.put("subordinateSystem", systemId); // 所属系统编码
                                                jsonObject.put("subordinateSystemNum", alarmData.getSystem()); // 所属系统描述
                                                jsonObject.put("eqType", asset.getClassificationId()); // 设备类型编码
                                                jsonObject.put("location", alarmData.getLocation()); // 故障位置描述
                                                jsonObject.put("equipment", alarmData.getAssetnum()); // 设备编码
                                                jsonObject.put("faultClassify", "02"); // 故障分类('01'--点位超限，'02'--设备故障,'03'--通讯掉线，‘04’--参数校核，‘05’--能耗问题，‘06’--能效问题，‘07’--设备运行，‘08’--日常保修，‘09’--其他)，和工单页面同步即可
                                                jsonObject.put("orderOrigin", "01"); // 工单来源 ('01'--自动提报，'02'--PC端手动提报，‘03’-- APP手动提报)，和工单页面同步即可
                                                jsonObject.put("orderLevel", "01"); // 工单级别 ('00'--紧急，'01'--高级，'02'--中级，'03'--低级)，和工单页面同步即可
                                                jsonObject.put("orgId", thinkbosAlarmConfigDto.getOrgId()); // 组织ID
                                                jsonObject.put("siteId", thinkbosAlarmConfigDto.getSiteId()); // 站点ID
                                                jsonObject.put("alarmDataId", alarmData.getId()); // 实时报警主键id

                                                // 苏北的报警直接指定到对应的班组，和其他项目处理逻辑不一致 zzx
                                                if (profilesActive.equalsIgnoreCase("subei")) {

                                                    // 查询报警配置的工单配置表
                                                    List<AlarmSmsConfig> smsConfigs = alarmSmsConfigRepository.findByAssetTypeIdAndOrgIdAndSiteId(asset.getClassificationId(), thinkbosAlarmConfigDto.getOrgId(), thinkbosAlarmConfigDto.getSiteId());
                                                    if (smsConfigs.size() > 0) {
                                                        AlarmSmsConfig alarmSmsConfig = smsConfigs.get(0);
                                                        // 处理执行班组
                                                        if (StringUtils.isNotBlank(alarmSmsConfig.getAutoAssignUserId()) && StringUtils.isNotBlank(alarmSmsConfig.getAutoAssignUserName())) {
                                                            jsonObject.put("dealGroup", alarmSmsConfig.getAutoAssignUserId()); // 班组id
                                                            jsonObject.put("dealGroupName", alarmSmsConfig.getAutoAssignUserName()); // 班组名
                                                            // 发送报警触发工单消息到队列中
                                                            alarmToWorkOrder.sendAlarmDataToWorkOrder(jsonObject.toString());
                                                        }
                                                    } else {
                                                        logger.debug("----send workorder----no config team----");
                                                    }

                                                } else {
                                                    if (alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_EXECUTOR) != null || alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_ACCEPTOR) != null) {
                                                        // 处理执行人
                                                        if (alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_EXECUTOR) != null && alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_EXECUTOR).size() > 0) {
                                                            List<ThinkbosAlarmConfigNote> executorConfigs = alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_EXECUTOR);
                                                            logger.debug("----send workorder----executorConfigs： {}----alarm tag: {}----", executorConfigs.size(), thinkbosAlarmConfigDto.getAlarmName());
                                                            jsonObject.put("executorId", executorConfigs.get(0).getReceiverId()); // 主执行人账号id
                                                            AuthoriseDeepCtrlsUser user = authoriseDeepCtrlsUserService.findAuthoriseDeepCtrlsUserById(executorConfigs.get(0).getReceiverId());
                                                            jsonObject.put("mainExecutor", user != null ? user.getUserName() : ""); // 主执行人姓名
                                                        }

                                                        // 处理验收人
                                                        if (alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_ACCEPTOR) != null && alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_ACCEPTOR).size() > 0) {
                                                            List<ThinkbosAlarmConfigNote> acceptConfigs = alarmLinkageConfigMap.get(ThinkbosAlarmConstant.WORK_ORDER_ACCEPTOR);
                                                            logger.debug("----send workorder----acceptConfigs： {}----alarm tag: {}----", acceptConfigs.size(), thinkbosAlarmConfigDto.getAlarmName());
                                                            jsonObject.put("acceptorId", acceptConfigs.get(0).getReceiverId()); // 验收人账号id
                                                            AuthoriseDeepCtrlsUser user = authoriseDeepCtrlsUserService.findAuthoriseDeepCtrlsUserById(acceptConfigs.get(0).getReceiverId());
                                                            jsonObject.put("acceptorName", user != null ? user.getUserName() : ""); // 验收人姓名
                                                        } else {
                                                            jsonObject.put("acceptorId", ""); // 验收人账号id
                                                            jsonObject.put("acceptorName", ""); // 验收人姓名
                                                        }

                                                        // 发送报警触发工单消息到队列中
                                                        alarmToWorkOrder.sendAlarmDataToWorkOrder(jsonObject.toString());
                                                    }
                                                }
                                            } catch (Exception e) {
                                                logger.error("alarm build workorder error ----{}----", e);
                                            }

                                            // 报警推送消息到app
                                            if (thinkbosAlarmConfigDto.getAppRelationStatus()) {
                                                try {
                                                    pushMsgToAppSender.sendMobileClientTitleMsgBySystemId(null, priorityName + "报警", alarmData.getAssetname().replaceAll("#", "_") + "发生了报警，" + alarmData.getDescription().replaceAll("#", "_"), AlarmConstant.PUSHTOAPPALARMTYPE, alarmData.getId(), systemId);
                                                } catch (Exception e) {
                                                    logger.error("send msg to app error ----{}----", e);
                                                }
                                            }
                                            //TODO 暂时添加发送微信请求
                                        /*SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                        String alarmTime = formatter.format(alarmData.getTime());
                                        this.weChatMpService.sendTemplateMessage(alarmData.getDescription(),alarmData.getAssetname(),alarmData.getAlarmtype(),alarmTime,alarmData.getDescription(),alarmData.getId(),"H5BA1DE49");*/
                                            if (thinkbosAlarmConfigDto.getWxMpStatus()) {
                                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                                JSONObject msgData = new JSONObject();
                                                msgData.put("description", alarmData.getDescription());
                                                msgData.put("assetName", alarmData.getAssetname());
                                                msgData.put("alarmType", alarmData.getAlarmtype());
                                                msgData.put("alarmTime", formatter.format(alarmData.getTime()));
                                                msgData.put("alarmMes", alarmData.getDescription());
                                                msgData.put("alarmId", alarmData.getId());
                                                msgData.put("objectId", wxObjectId);
                                                pushMsgToWeChatSender.sendMsgToWeChatByUserId(null, msgData, systemId);
                                            }


                                            //<editor-fold desc="判断报警是否发送给FM系统">
                                            logger.debug("判断报警是否发送给FM系统---开启状态---catlDockingFmEnable：【{}】，设备注册地址：【{}】，报警推送地址：【{}】", catlDockingFmEnable, catlDockingFmRegisterDeviceUrl, catlDockingFmPushDeviceAlarmUrl);
                                            if (catlDockingFmEnable) {
                                                try {
                                                    // 设备序列号，对方系统的字段设符长度置字为30位，我们是32位； 对方刁难不改，现在换成设备编码；因为设备编码也是唯一的
                                                    // 厂房编号, FM指定的对应关系，详见 CatlFmSiteCodeEnum
                                                    String fmLocationsId = "";
                                                    // 1、调用注册接口，将此设备注册
                                                    if (StringUtils.isBlank(catlDockingFmRegisterDeviceUrl)) {
                                                        logger.warn("报警发送给FM系统---已开启---但是没有配置FM设备注册地址");
                                                    } else {

                                                        // 判断设备是否已经注册过 ; 先判断是否在redis，不在再查表  保存在表；redis
                                                        String assetNum = asset.getAssetNum();
                                                        String assetName = asset.getAssetName();
                                                        String assetDescription = alarmData.getAssetname();
                                                        if (!redisClient.hexists(RedisConstant.ASSET_REGISTER_FM, assetNum)) {
                                                            String valueJson;
                                                            Optional<AssetRegisterFmPO> byId = assetRegisterFmRepository.findById(assetNum);
                                                            if (byId.isPresent()) {
                                                                // 存在表，但是不在redis中，需要放到redis中
                                                                valueJson = JSONUtil.toJsonStr(byId.get());
                                                                redisClient.hset(RedisConstant.ASSET_REGISTER_FM, assetNum, valueJson);
                                                            } else {
                                                                // 不在表里，也不在redis；需要保存到表里，并保存到redis中，并且发送给fm系统（调用设备注册接口）
                                                                // 1、调用FM注册接口
                                                                FmRegisterDeviceRequest registerParam = new FmRegisterDeviceRequest();
                                                                // 设备序列
                                                                registerParam.setSerialNumber(assetNum);
                                                                // 厂房编号, FM指定的对应关系，详见 CatlFmSiteCodeEnum
                                                                if (StrUtil.isNotBlank(asset.getLocationsText())) {
                                                                    String key = StrUtil.split(asset.getLocationsText(), "-")[0];
                                                                    fmLocationsId = CatlFmSiteCodeEnum.getDataMap().get(key);
                                                                }
                                                                registerParam.setSiteCode(fmLocationsId);

                                                                // 设备描述
                                                                /*registerParam.setDeviceName(alarmData.getAssetname());*/
                                                                registerParam.setDeviceName(assetName);
                                                                String registerParamJson = JSONUtil.toJsonStr(registerParam);
                                                                logger.debug("调用FM设备注册接口,将设备注册到FM系统中---请求地址：{}---入参：{}", catlDockingFmRegisterDeviceUrl, registerParamJson);
                                                                String registerPostRest = HttpUtil.post(catlDockingFmRegisterDeviceUrl, registerParamJson, 8000);
                                                                logger.debug("调用FM设备注册接口,将设备注册到FM系统中---返回：{}", registerPostRest);
                                                                if (JSONUtil.isJson(registerPostRest)) {
                                                                    cn.hutool.json.JSONObject jsonObject = JSONUtil.parseObj(registerPostRest);
                                                                    String status = jsonObject.getStr("status");
                                                                    if (StrUtil.equals(status,"200")) {
                                                                        // 2、保存到表
                                                                        String orgId = thinkbosAlarmConfigDto.getOrgId();
                                                                        String siteId = thinkbosAlarmConfigDto.getSiteId();
                                                                        AssetRegisterFmPO savePo = new AssetRegisterFmPO(assetNum, assetName, assetDescription, orgId, siteId, DateUtil.getCurrentDateTime());
                                                                        assetRegisterFmRepository.save(savePo);
                                                                        valueJson = JSONUtil.toJsonStr(savePo);
                                                                        //保存到redis
                                                                        redisClient.hset(RedisConstant.ASSET_REGISTER_FM, assetNum, valueJson);
                                                                    }else{
                                                                        logger.error("调用FM设备注册接口,将设备注册到FM系统中返回结果错误!---{}", registerPostRest);
                                                                    }
                                                                }
                                                            }

                                                        } else {
                                                            logger.debug("---FM设备注册接口---设备已经注册过，不用再调用！---设备编号：【{}】", assetNum);
                                                        }
                                                    }

                                                    // 2、调用推送报警接口，推送此报警
                                                    if (StringUtils.isBlank(catlDockingFmPushDeviceAlarmUrl)) {
                                                        logger.warn("报警发送给FM系统---已开启---但是没有配置FM推送报警地址");
                                                    } else {
                                                        FmPushDeviceAlarmRequest pushParam = new FmPushDeviceAlarmRequest();
                                                        // 设备序列 （逻辑编号）
                                                        pushParam.setSerialNumber(asset.getAssetNum());
                                                        // 厂房区域,  FM指定的对应关系，详见 CatlFmSiteCodeEnum
                                                        if (StrUtil.isNotBlank(asset.getLocationsText())) {
                                                            String key = StrUtil.split(asset.getLocationsText(), "-")[0];
                                                            fmLocationsId = CatlFmSiteCodeEnum.getDataMap().get(key);
                                                        }
                                                        pushParam.setSiteCode(fmLocationsId);
                                                        // 报警等级
                                                        // FM指定的对应关系，详见 CatlFmAlterTypeEnum
                                                        pushParam.setAlterType(CatlFmAlterTypeEnum.getDataMap().get(priorityName));
                                                        // 报警内容
                                                        pushParam.setAlterContent(alarmDesc);
                                                        // 报警时间
                                                        pushParam.setAlterTime(DateUtil.format(alarmData.getTime(), DateUtil.yyyyMMddHHmmss));
                                                        //同步校验 ，同步，“Y”，“N”
                                                        pushParam.setSync("Y");
                                                        // FM需要组装成集合给他们
                                                        List<FmPushDeviceAlarmRequest> pushParamList = new ArrayList<>();
                                                        pushParamList.add(pushParam);
                                                        String pushParamListJson = JSONUtil.toJsonStr(pushParamList);
                                                        logger.debug("调用FM推送报警接口，推送此报警到FM中---请求地址：{}---入参：{}", catlDockingFmPushDeviceAlarmUrl, pushParamListJson);
                                                        String pushPostRest = HttpUtil.post(catlDockingFmPushDeviceAlarmUrl, pushParamListJson, 8000);
                                                        logger.debug("调用FM推送报警接口，推送此报警到FM中---返回：{}", pushPostRest);
                                                    }
                                                } catch (Exception e) {
                                                    logger.error("alarm send FM error", e);
                                                }
                                            }
                                            //</editor-fold>


                                        }

                                    }
                                } else {
                                    logger.error("----saveOrUpdateSightAlarmData----asset is null----点位[{}]从sight台帐文件中未找到相应记录----{}", thinkbosAlarmConfigDto.getAlarmName());
                                }
                            } catch (Exception e) {
                                logger.error("----saveOrUpdateSightAlarmData----点位[{}]从sight台帐文件中未找到相应记录----{}", thinkbosAlarmConfigDto.getAlarmName(), e);
                            }
                        }
                    } else {
                        logger.warn("----saveOrUpdateSightAlarmData----点位[{}]在未关闭报警中有对应的记录，不需要再次处理--------", thinkbosAlarmConfigDto.getAlarmName());
                    }
                }
            }
        } catch (Exception ee) {
            logger.error("----saveOrUpdateSightAlarmData----最外层catch----error----{}", ee);
        }
        return alarmData;
    }


    /**
     * 发送数据到博纳瑞通平台
     *
     * @param billType     业务消息类型，报警/恢复
     * @param alarmType    报警类型
     * @param alarmContent 报警内容
     */
    private void sendAlarmDataToBnrtPlatform(String billType, String alarmType, String alarmContent) {
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("billNo", ""); // 业务编码，如果后期需要根据业务编码来查找相关业务则传此字段
        paramMap.put("billType", billType); // 业务消息类型，报警/恢复
        paramMap.put("alarmType", alarmType); // 报警类型
        paramMap.put("alarmContent", alarmContent); // 报警内容
        String reponseString = DeepCtrlsHttpUtils.doPostJsonParams(sendBnrtAlarmUrl, paramMap);
        logger.info("----sendAlarmDataToBnrtPlatform----{}----", reponseString);
    }

    /**
     * 获取指定格式的日期
     *
     * @param format 格式化字符串（缺省为：yyyy-MM-dd）
     * @return
     */
    private String getCurrentDate(String format) {
        Calendar calendar = Calendar.getInstance();
        if (StringUtils.isBlank(format)) {
            format = "yyyy-MM-dd";
        }
        return new SimpleDateFormat(format).format(calendar.getTime());
    }

    /**
     * 判断到屏蔽时间已过时，恢复屏蔽状态为未屏蔽
     *
     * @param alarmData
     * @param action    true 代表触发报警   false代表恢复报警
     * @return
     */
    private AlarmData modifyAdRestrain(AlarmData alarmData, Boolean action) {
        if ((alarmData.getRestrain() == 1L) && alarmData.getRestrainEndTime().before(new Date())) {
            alarmData.setRestrain(0L);
            if (action) {
                alarmData.setRestrainNum((null != alarmData.getRestrainNum() ? alarmData.getRestrainNum() : 0) + 1);
            }
            alarmService.updateRestrainHistory(alarmData.getTagname(), new Date());
            return alarmData;
        }
        return alarmData;
    }

    /**
     * 判断报警是否屏蔽，且屏蔽时间是否达到
     *
     * @param restrain
     * @param restrainEndTime
     * @return
     */
    private Boolean judgeAdRestrain(Long restrain, Date restrainEndTime) {
        Date date = new Date();
        if ((restrain == 1L) && restrainEndTime.after(date)) {
            return false;
        } else {
            return true;
        }
    }

    // java 字符串按大小（占字节数）切分
    public static List<String> splitBySize(String value, int length) {
        try {
            if (value == null) {
                return null;
            }
            List<String> splitList = new ArrayList<String>();
            int startIndex = 0;    //字符串截取起始位置
            int endIndex = length > value.length() ? value.length() : length;  //字符串截取结束位置
            while (startIndex < value.length()) {
                String subString = value.substring(startIndex, endIndex);
                //截取的字符串的字节长度大于需要截取的长度时，说明包含中文字符
                //在GBK编码中，一个中文字符占2个字节，UTF-8编码格式，一个中文字符占3个字节。
                while (subString.getBytes("GBK").length > length) {
                    --endIndex;
                    subString = value.substring(startIndex, endIndex);
                }
                splitList.add(value.substring(startIndex, endIndex));
                startIndex = endIndex;
                //判断结束位置时要与字符串长度比较(src.length())，之前与字符串的bytes长度比较了，导致越界异常。
                endIndex = (startIndex + length) > value.length() ?
                        value.length() : startIndex + length;

            }
            return splitList;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }
}
