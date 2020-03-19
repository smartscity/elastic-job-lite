package com.dangdang.ddframe.job.lite.internal.schedule;

import com.dangdang.ddframe.job.exception.JobSystemException;
import lombok.RequiredArgsConstructor;
import org.quartz.*;

import java.util.Date;

@RequiredArgsConstructor
public final class JobScheduleController {
    
    private final Scheduler scheduler;
    
    private final JobDetail jobDetail;
    
    private final String triggerIdentity;

    /**
     * 毫秒级别作业 配置作业调度表达式的分隔符
	 * 分号前面是： initialDelay 初始化等待时间
	 * 分号后面是： period/delay 间隔调度的毫秒数
     */
    private static final String MILLISECOND_JOB_EXPRESSION_SEPARATOR = ";";

    /**
     * 调度作业.
     * 
     * @param cron CRON表达式
     */
    public void scheduleJob(final String cron) {
        try {
            if (!scheduler.checkExists(jobDetail.getKey())) {
                scheduler.scheduleJob(jobDetail, isSimpleTriggerJob(cron) ? createSimpleTrigger(cron) : createTrigger(cron));
            }
            scheduler.start();
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * 重新调度作业.
     * 
     * @param cron CRON表达式
     */
    public synchronized void rescheduleJob(final String cron) {
        if (isSimpleTriggerJob(cron)) {
            rescheduleSimpleTriggerJob(cron);
        } else {
            rescheduleCronTriggerJob(cron);
        }
    }

    /**
     * 重新调度作业.
     *
     * @param cron CRON表达式
     */
    public void rescheduleCronTriggerJob(final String cron) {
        try {
            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger && !cron.equals(trigger.getCronExpression())) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createTrigger(cron));
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 重新调度作业. 毫秒级别作业
     *
     * @param cron CRON表达式
     */
    public void rescheduleSimpleTriggerJob(final String cron) {
        try {
            SimpleTrigger trigger = (SimpleTrigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger && !cron.equals(trigger.getStartTime().getTime() + MILLISECOND_JOB_EXPRESSION_SEPARATOR + trigger.getRepeatInterval())) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createSimpleTrigger(cron));
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    private CronTrigger createTrigger(final String cron) {
        return TriggerBuilder.newTrigger().withIdentity(triggerIdentity).withSchedule(CronScheduleBuilder.cronSchedule(cron).withMisfireHandlingInstructionDoNothing()).build();
    }

    private SimpleTrigger createSimpleTrigger(final String cron) {
        String[] arr = cron.split(MILLISECOND_JOB_EXPRESSION_SEPARATOR);
        return TriggerBuilder.newTrigger().startAt(new Date(System.currentTimeMillis() + Long.parseLong(arr[0]))).withIdentity(triggerIdentity).withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(Long.parseLong(arr[1])).repeatForever().withMisfireHandlingInstructionNowWithExistingCount()).build();
    }

    /**
     * 判断作业是否暂停.
     * 
     * @return 作业是否暂停
     */
    public synchronized boolean isPaused() {
        try {
            return !scheduler.isShutdown() && Trigger.TriggerState.PAUSED == scheduler.getTriggerState(new TriggerKey(triggerIdentity));
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * 暂停作业.
     */
    public synchronized void pauseJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.pauseAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * 恢复作业.
     */
    public synchronized void resumeJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.resumeAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * 立刻启动作业.
     */
    public synchronized void triggerJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.triggerJob(jobDetail.getKey());
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * 关闭调度器.
     */
    public synchronized void shutdown() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 判断是否是毫秒级别的作业
     * @param cron
     * @return
     */
    private boolean isSimpleTriggerJob(final String cron) {
        return cron.contains(MILLISECOND_JOB_EXPRESSION_SEPARATOR);
    }
}