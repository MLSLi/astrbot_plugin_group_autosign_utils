# astrbot_plugin_group_autosign_utils

## 简介
群组自动打卡续火插件

### 平台
QQ群聊 (Aiocqhttp)

### 支持的功能
- 随机续火时间防止被风控
- 自定义每日打卡时间和打卡间隔

## 配置
| 配置名      | 描述 | 默认值     |
|    :---:    |    :----:   |     :---:     |
| autosign_daily_time | 每天打卡开始时间 | 00-00-00 |
| auto_send_msg_random_time_range | 随机时间续火时间区间 | "9-23" |
| autosign_group_list | 每日打卡群号列表 | 空 |
| auto_send_msg_group_list | 每日续火群号列表 | 空 |
| autosign_delay | 每日续火延迟(秒) | 2.0 |


### 注意事项
- 在插件每次启动或者重载后，请在Bot在的任意群发送任意动作(撤回消息不算)，或者直接私聊，这样才能正常工作

## 更新日志

### v1.0.0
- 初始版本

# 支持

- [插件开发文档](https://docs.astrbot.app/dev/star/plugin-new.html)
