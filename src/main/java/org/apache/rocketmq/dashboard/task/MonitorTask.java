/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.dashboard.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.dashboard.config.RMQNotifyConfigure;
import org.apache.rocketmq.dashboard.model.ConsumerMonitorConfig;
import org.apache.rocketmq.dashboard.model.GroupConsumeInfo;
import org.apache.rocketmq.dashboard.service.ConsumerService;
import org.apache.rocketmq.dashboard.service.MonitorService;
import org.apache.rocketmq.dashboard.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

@Component
public class MonitorTask {
    private Logger logger = LoggerFactory.getLogger(MonitorTask.class);

    @Resource
    private MonitorService monitorService;

    @Resource
    private ConsumerService consumerService;

    @Resource
    private RestTemplate restTemplate;

    @Resource
    private JavaMailSender mailSender;

    @Resource
    private RMQNotifyConfigure notifyConfig;

    @Scheduled(cron = "0 0/1 * * * ?")
    public void scanProblemConsumeGroup() {
        for (Map.Entry<String, ConsumerMonitorConfig> configEntry : monitorService.queryConsumerMonitorConfig().entrySet()) {
            GroupConsumeInfo consumeInfo = consumerService.queryGroup(configEntry.getKey());
            if (consumeInfo.getCount() < configEntry.getValue().getMinCount() || consumeInfo.getDiffTotal() > configEntry.getValue().getMaxDiffTotal()) {
                logger.info("op=look consumeInfo {}", JsonUtil.obj2String(consumeInfo));
                // notify the alert system
                String text = this.getText(consumeInfo);
                if (notifyConfig.getMail().isEnabled()) {
                    this.sendMail(text);
                }

                if (notifyConfig.getDingTalk().isEnabled()) {
                    this.sendDingTalk(text);
                }
            }
        }
    }

    private String getText(GroupConsumeInfo consume) {
        String now = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINA).format(ZonedDateTime.now());
        return String.format("【告警】RocketMQ\n订阅组：%s\n消费者数量：%s\n堆积数量：%s\n告警时间：%s",
                consume.getGroup(), consume.getCount(), consume.getDiffTotal(), now);
    }

    private void sendDingTalk(String text) {
        try {
            Map<String, String> contentMap = Maps.newHashMap();
            contentMap.put("content", text);

            Map<String, Object> parameters = Maps.newHashMap();
            parameters.put("msgtype", "text");
            parameters.put("text", contentMap);

            String requestJson = new ObjectMapper().writeValueAsString(parameters);

            String url = String.format("https://oapi.dingtalk.com/robot/send?access_token=%s", notifyConfig.getDingTalk().getAccessToken());
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(requestJson, headers);
            restTemplate.postForObject(url, entity, String.class);
        } catch (Exception e) {
            logger.error("Send Notify Mail Error: ", e);
        }

    }

    private void sendMail(String text) {
        try {
            RMQNotifyConfigure.MailConfig mail = notifyConfig.getMail();
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(mail.getFrom()); //邮件发送人
            message.setTo(StringUtils.split(mail.getTo(), ",")); //邮件接收人
            message.setSubject("【告警】RocketMQ"); //邮件主题
            message.setText(text); //邮件内容
            mailSender.send(message); //发送邮件
        } catch (Exception e) {
            logger.error("Send Notify Mail Error: ", e);
        }
    }

}
