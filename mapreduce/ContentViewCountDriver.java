package com.media.warehouse.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Production MapReduce Driver for Multi-Dimensional Media Content Consumption Analysis.
 *
 * <p>Aggregates media viewing counts, total streaming duration, and completion rates
 * across three analytical dimensions simultaneously:
 * <ul>
 *   <li>Content Genre (e.g., Action, Sci-Fi, Drama)</li>
 *   <li>Time Period (e.g., YYYY-MM-DD or hourly bucket)</li>
 *   <li>Device Category (e.g., SmartTV, Mobile, Web, Console)</li>
 * </ul>
 *
 * <p>Architectural highlights:
 * <ul>
 *   <li>Custom {@link ContentViewCompositeKey} implementing {@link WritableComparable}</li>
 *   <li>Custom {@link ContentViewMetricsWritable} for multi-metric aggregation</li>
 *   <li>Local in-memory Combiner to minimize shuffle I/O and network serialization</li>
 *   <li>Configurable CLI options via Hadoop {@link Tool} and {@link ToolRunner}</li>
 *   <li>Granular operational counters for data quality auditing</li>
 * </ul>
 */
public class ContentViewCountDriver extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ContentViewCountDriver.class);

    // Operational audit counters
    public enum MetricCounters {
        TOTAL_RECORDS,
        VALID_RECORDS,
        CORRUPT_RECORDS,
        MISSING_DIMENSIONS,
        ZERO_DURATION_RECORDS
    }

    // =========================================================================
    // 1. Composite Key Definition: (Genre, TimePeriod, DeviceType)
    // =========================================================================
    public static class ContentViewCompositeKey implements WritableComparable<ContentViewCompositeKey> {

        private Text genre;
        private Text timePeriod;
        private Text deviceType;

        public ContentViewCompositeKey() {
            this.genre = new Text();
            this.timePeriod = new Text();
            this.deviceType = new Text();
        }

        public ContentViewCompositeKey(String genre, String timePeriod, String deviceType) {
            this.genre = new Text(genre != null ? genre : "UNKNOWN");
            this.timePeriod = new Text(timePeriod != null ? timePeriod : "UNKNOWN");
            this.deviceType = new Text(deviceType != null ? deviceType : "UNKNOWN");
        }

        public void set(String genre, String timePeriod, String deviceType) {
            this.genre.set(genre != null ? genre : "UNKNOWN");
            this.timePeriod.set(timePeriod != null ? timePeriod : "UNKNOWN");
            this.deviceType.set(deviceType != null ? deviceType : "UNKNOWN");
        }

        public Text getGenre() { return genre; }
        public Text getTimePeriod() { return timePeriod; }
        public Text getDeviceType() { return deviceType; }

        @Override
        public void write(DataOutput out) throws IOException {
            genre.write(out);
            timePeriod.write(out);
            deviceType.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            genre.readFields(in);
            timePeriod.readFields(in);
            deviceType.readFields(in);
        }

        @Override
        public int compareTo(ContentViewCompositeKey other) {
            int cmp = this.genre.compareTo(other.genre);
            if (cmp != 0) return cmp;

            cmp = this.timePeriod.compareTo(other.timePeriod);
            if (cmp != 0) return cmp;

            return this.deviceType.compareTo(other.deviceType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(genre.toString(), timePeriod.toString(), deviceType.toString());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ContentViewCompositeKey that = (ContentViewCompositeKey) obj;
            return Objects.equals(genre, that.genre) &&
                   Objects.equals(timePeriod, that.timePeriod) &&
                   Objects.equals(deviceType, that.deviceType);
        }

        @Override
        public String toString() {
            return genre.toString() + "\t" + timePeriod.toString() + "\t" + deviceType.toString();
        }
    }

    // =========================================================================
    // 2. Metrics Value Writable Definition: (ViewCount, WatchSeconds, Completions)
    // =========================================================================
    public static class ContentViewMetricsWritable implements Writable {

        private long viewCount;
        private long watchDurationSec;
        private long completionCount;

        public ContentViewMetricsWritable() {
            this.viewCount = 0L;
            this.watchDurationSec = 0L;
            this.completionCount = 0L;
        }

        public ContentViewMetricsWritable(long viewCount, long watchDurationSec, long completionCount) {
            this.viewCount = viewCount;
            this.watchDurationSec = watchDurationSec;
            this.completionCount = completionCount;
        }

        public void set(long viewCount, long watchDurationSec, long completionCount) {
            this.viewCount = viewCount;
            this.watchDurationSec = watchDurationSec;
            this.completionCount = completionCount;
        }

        public long getViewCount() { return viewCount; }
        public long getWatchDurationSec() { return watchDurationSec; }
        public long getCompletionCount() { return completionCount; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(viewCount);
            out.writeLong(watchDurationSec);
            out.writeLong(completionCount);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            viewCount = in.readLong();
            watchDurationSec = in.readLong();
            completionCount = in.readLong();
        }

        @Override
        public String toString() {
            return viewCount + "\t" + watchDurationSec + "\t" + completionCount;
        }
    }

    // =========================================================================
    // 3. Mapper Implementation
    // =========================================================================
    public static class ContentViewMapper
            extends Mapper<LongWritable, Text, ContentViewCompositeKey, ContentViewMetricsWritable> {

        private final ContentViewCompositeKey outKey = new ContentViewCompositeKey();
        private final ContentViewMetricsWritable outValue = new ContentViewMetricsWritable();

        // Expected input format (TSV):
        // 0: view_id
        // 1: user_id
        // 2: content_id
        // 3: genre
        // 4: time_period (e.g., "2026-11-14" or "2026-11-14 10:00")
        // 5: device_type (e.g., "SMART_TV", "MOBILE_IOS")
        // 6: watch_duration_sec
        // 7: is_completed (1 or 0)
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(MetricCounters.TOTAL_RECORDS).increment(1);

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) {
                return; // Skip empty lines and headers
            }

            String[] tokens = line.split("\t");
            if (tokens.length < 7) {
                // Try fallback to CSV if tab split didn't yield enough tokens
                tokens = line.split(",");
                if (tokens.length < 7) {
                    context.getCounter(MetricCounters.CORRUPT_RECORDS).increment(1);
                    return;
                }
            }

            try {
                String genre = tokens[3].trim();
                String timePeriod = tokens[4].trim();
                String deviceType = tokens[5].trim();

                // Validate dimensions
                if (genre.isEmpty() || timePeriod.isEmpty() || deviceType.isEmpty()) {
                    context.getCounter(MetricCounters.MISSING_DIMENSIONS).increment(1);
                    return;
                }

                // If timestamp contains hours/minutes, extract date or normalize
                if (timePeriod.length() > 10 && timePeriod.contains("T")) {
                    timePeriod = timePeriod.substring(0, 10);
                } else if (timePeriod.length() > 10 && timePeriod.contains(" ")) {
                    timePeriod = timePeriod.substring(0, 10);
                }

                long watchDurationSec = 0L;
                if (!tokens[6].trim().isEmpty()) {
                    watchDurationSec = Long.parseLong(tokens[6].trim());
                }

                long isCompleted = 0L;
                if (tokens.length > 7 && !tokens[7].trim().isEmpty()) {
                    String compStr = tokens[7].trim();
                    if ("1".equals(compStr) || "true".equalsIgnoreCase(compStr)) {
                        isCompleted = 1L;
                    }
                }

                if (watchDurationSec <= 0) {
                    context.getCounter(MetricCounters.ZERO_DURATION_RECORDS).increment(1);
                }

                outKey.set(genre.toUpperCase(), timePeriod, deviceType.toUpperCase());
                outValue.set(1L, watchDurationSec, isCompleted);

                context.write(outKey, outValue);
                context.getCounter(MetricCounters.VALID_RECORDS).increment(1);

            } catch (NumberFormatException nfe) {
                context.getCounter(MetricCounters.CORRUPT_RECORDS).increment(1);
            }
        }
    }

    // =========================================================================
    // 4. Combiner Implementation (Local Pre-Aggregation)
    // =========================================================================
    public static class ContentViewCombiner
            extends Reducer<ContentViewCompositeKey, ContentViewMetricsWritable,
                            ContentViewCompositeKey, ContentViewMetricsWritable> {

        private final ContentViewMetricsWritable combinedValue = new ContentViewMetricsWritable();

        @Override
        protected void reduce(ContentViewCompositeKey key, Iterable<ContentViewMetricsWritable> values, Context context)
                throws IOException, InterruptedException {

            long totalViews = 0L;
            long totalDuration = 0L;
            long totalCompletions = 0L;

            for (ContentViewMetricsWritable val : values) {
                totalViews += val.getViewCount();
                totalDuration += val.getWatchDurationSec();
                totalCompletions += val.getCompletionCount();
            }

            combinedValue.set(totalViews, totalDuration, totalCompletions);
            context.write(key, combinedValue);
        }
    }

    // =========================================================================
    // 5. Reducer Implementation (Global Dimensional Aggregation)
    // =========================================================================
    public static class ContentViewReducer
            extends Reducer<ContentViewCompositeKey, ContentViewMetricsWritable,
                            ContentViewCompositeKey, Text> {

        private final Text resultValue = new Text();

        @Override
        protected void reduce(ContentViewCompositeKey key, Iterable<ContentViewMetricsWritable> values, Context context)
                throws IOException, InterruptedException {

            long totalViews = 0L;
            long totalDurationSec = 0L;
            long totalCompletions = 0L;

            for (ContentViewMetricsWritable val : values) {
                totalViews += val.getViewCount();
                totalDurationSec += val.getWatchDurationSec();
                totalCompletions += val.getCompletionCount();
            }

            double avgDurationMins = 0.0;
            double completionPercentage = 0.0;

            if (totalViews > 0) {
                avgDurationMins = Math.round(((double) totalDurationSec / 60.0 / totalViews) * 100.0) / 100.0;
                completionPercentage = Math.round(((double) totalCompletions / totalViews * 100.0) * 100.0) / 100.0;
            }

            // Output format:
            // totalViews \t totalDurationSec \t avgDurationMins \t totalCompletions \t completionPercentage%
            String formattedMetrics = String.format("%d\t%d\t%.2f\t%d\t%.2f%%",
                    totalViews,
                    totalDurationSec,
                    avgDurationMins,
                    totalCompletions,
                    completionPercentage);

            resultValue.set(formattedMetrics);
            context.write(key, resultValue);
        }
    }

    // =========================================================================
    // 6. Tool Runner & Job Configuration
    // =========================================================================
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (remainingArgs.length < 2) {
            System.err.println("Usage: ContentViewCountDriver [generic options] <input_path> <output_path> [num_reducers]");
            System.err.println("  input_path   : HDFS directory containing content view logs");
            System.err.println("  output_path  : HDFS destination directory for aggregated results");
            System.err.println("  num_reducers : (Optional) Number of reduce tasks (default: 8)");
            return -1;
        }

        Path inputPath = new Path(remainingArgs[0]);
        Path outputPath = new Path(remainingArgs[1]);
        int numReducers = (remainingArgs.length >= 3) ? Integer.parseInt(remainingArgs[2]) : 8;

        // Optimization: Map output compression
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "MediaWarehouse_ContentViewCount_Aggregation");
        job.setJarByClass(ContentViewCountDriver.class);

        // Input & Output Formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Mapper, Combiner, Reducer classes
        job.setMapperClass(ContentViewMapper.class);
        job.setCombinerClass(ContentViewCombiner.class);
        job.setReducerClass(ContentViewReducer.class);

        // Map Output Key / Value
        job.setMapOutputKeyClass(ContentViewCompositeKey.class);
        job.setMapOutputValueClass(ContentViewMetricsWritable.class);

        // Final Output Key / Value
        job.setOutputKeyClass(ContentViewCompositeKey.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(numReducers);

        LOG.info("Submitting MapReduce Job [ContentViewCountDriver]...");
        LOG.info("Input Path    : {}", inputPath);
        LOG.info("Output Path   : {}", outputPath);
        LOG.info("Num Reducers  : {}", numReducers);

        boolean success = job.waitForCompletion(true);

        if (success) {
            LOG.info("Job completed successfully. Auditing metric counters:");
            for (MetricCounters c : MetricCounters.values()) {
                Counter counter = job.getCounters().findCounter(c);
                LOG.info("  {}: {}", counter.getDisplayName(), counter.getValue());
            }
            return 0;
        } else {
            LOG.error("Job execution failed.");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ContentViewCountDriver(), args);
        System.exit(exitCode);
    }
}
