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
package org.apache.rocketmq.dashboard.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
@ConfigurationProperties(prefix = "rocketmq.notify")
public class RMQNotifyConfigure {
    private MailConfig mail;
    private DingTalkConfig dingTalk;

    public RMQNotifyConfigure() {
        mail = new MailConfig();
        dingTalk = new DingTalkConfig();
    }

    public MailConfig getMail() {
        return mail;
    }

    public void setMail(MailConfig mail) {
        this.mail = mail;
    }

    public DingTalkConfig getDingTalk() {
        return dingTalk;
    }

    public void setDingTalk(DingTalkConfig dingTalk) {
        this.dingTalk = dingTalk;
    }

    public class MailConfig {
        private boolean enabled = false;
        private String from;
        private String to;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }
    }

    public class DingTalkConfig {
        private boolean enabled = false;
        private String accessToken;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public void setAccessToken(String accessToken) {
            this.accessToken = accessToken;
        }
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}
