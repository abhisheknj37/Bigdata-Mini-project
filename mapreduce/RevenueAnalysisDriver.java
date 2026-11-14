package com.media.warehouse.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Production MapReduce Driver for Advertising Revenue & Pacing Analysis in Media & Entertainment.
 *
 * <p>Analyzes advertising monetization across:
 * <ul>
 *   <li>Advertiser Identifier (e.g., ADV_1001)</li>
 *   <li>Content Type (e.g., VOD_MOVIE, LIVE_STREAM, SHORT_CLIP)</li>
 *   <li>Geographic Market / Region (e.g., US_EAST, EMEA, APAC)</li>
 * </ul>
 *
 * <p>Advanced MapReduce Design Patterns:
 * <ul>
 *   <li><b>Secondary Sort Pattern:</b> Orders ad impression events chronologically per (Advertiser, ContentType, Region)
 *       stream without in-memory buffering.</li>
 *   <li><b>Custom Partitioner:</b> Partitions records exclusively by Natural Key (Advertiser + ContentType + Region)
 *       to guarantee all chronological events for an advertiser market reach the same reduce task.</li>
 *   <li><b>Custom Sort Comparator:</b> Sorts by Natural Key ascending, then Event Timestamp ascending.</li>
 *   <li><b>Custom Grouping Comparator:</b> Groups records arriving at the Reducer by Natural Key only,
 *       enabling a continuous time-series streaming reduction.</li>
 * </ul>
 */
public class RevenueAnalysisDriver extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(RevenueAnalysisDriver.class);

    public enum RevenueCounters {
        TOTAL_INPUT_EVENTS,
        VALID_AD_EVENTS,
        INVALID_PRICE_EVENTS,
        CORRUPT_FORMAT_RECORDS,
        HIGH_VALUE_IMPRESSIONS
    }

    // =========================================================================
    // 1. Composite Key Definition (Natural Key + Secondary Sort Timestamp)
    // =========================================================================
    public static class RevenueCompositeKey implements WritableComparable<RevenueCompositeKey> {

        // Natural key dimensions
        private Text advertiserId;
        private Text contentType;
        private Text region;

        // Secondary sort dimension (Unix epoch timestamp in milliseconds)
        private long eventTimestamp;

        public RevenueCompositeKey() {
            this.advertiserId = new Text();
            this.contentType = new Text();
            this.region = new Text();
            this.eventTimestamp = 0L;
        }

        public RevenueCompositeKey(String advertiserId, String contentType, String region, long eventTimestamp) {
            this.advertiserId = new Text(advertiserId != null ? advertiserId : "UNKNOWN");
            this.contentType = new Text(contentType != null ? contentType : "UNKNOWN");
            this.region = new Text(region != null ? region : "UNKNOWN");
            this.eventTimestamp = eventTimestamp;
        }

        public void set(String advertiserId, String contentType, String region, long eventTimestamp) {
            this.advertiserId.set(advertiserId != null ? advertiserId : "UNKNOWN");
            this.contentType.set(contentType != null ? contentType : "UNKNOWN");
            this.region.set(region != null ? region : "UNKNOWN");
            this.eventTimestamp = eventTimestamp;
        }

        public Text getAdvertiserId() { return advertiserId; }
        public Text getContentType() { return contentType; }
        public Text getRegion() { return region; }
        public long getEventTimestamp() { return eventTimestamp; }

        @Override
        public void write(DataOutput out) throws IOException {
            advertiserId.write(out);
            contentType.write(out);
            region.write(out);
            out.writeLong(eventTimestamp);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            advertiserId.readFields(in);
            contentType.readFields(in);
            region.readFields(in);
            eventTimestamp = in.readLong();
        }

        /**
         * Full Composite Comparison:
         * 1. advertiserId ASC
         * 2. contentType ASC
         * 3. region ASC
         * 4. eventTimestamp ASC (Chronological Order)
         */
        @Override
        public int compareTo(RevenueCompositeKey other) {
            int cmp = this.advertiserId.compareTo(other.advertiserId);
            if (cmp != 0) return cmp;

            cmp = this.contentType.compareTo(other.contentType);
            if (cmp != 0) return cmp;

            cmp = this.region.compareTo(other.region);
            if (cmp != 0) return cmp;

            return Long.compare(this.eventTimestamp, other.eventTimestamp);
        }

        @Override
        public int hashCode() {
            // Hash across the natural key and timestamp
            return Objects.hash(advertiserId.toString(), contentType.toString(), region.toString(), eventTimestamp);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            RevenueCompositeKey that = (RevenueCompositeKey) obj;
            return eventTimestamp == that.eventTimestamp &&
                   Objects.equals(advertiserId, that.advertiserId) &&
                   Objects.equals(contentType, that.contentType) &&
                   Objects.equals(region, that.region);
        }

        @Override
        public String toString() {
            return advertiserId.toString() + "\t" + contentType.toString() + "\t" + region.toString();
        }
    }

    // =========================================================================
    // 2. Value Writable: Ad Monetization Metrics
    // =========================================================================
    public static class RevenueEventWritable implements Writable {

        private double revenue;
        private long impressions;
        private long clicks;
        private long timestamp;

        public RevenueEventWritable() {
            this.revenue = 0.0;
            this.impressions = 0L;
            this.clicks = 0L;
            this.timestamp = 0L;
        }

        public RevenueEventWritable(double revenue, long impressions, long clicks, long timestamp) {
            this.revenue = revenue;
            this.impressions = impressions;
            this.clicks = clicks;
            this.timestamp = timestamp;
        }

        public void set(double revenue, long impressions, long clicks, long timestamp) {
            this.revenue = revenue;
            this.impressions = impressions;
            this.clicks = clicks;
            this.timestamp = timestamp;
        }

        public double getRevenue() { return revenue; }
        public long getImpressions() { return impressions; }
        public long getClicks() { return clicks; }
        public long getTimestamp() { return timestamp; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(revenue);
            out.writeLong(impressions);
            out.writeLong(clicks);
            out.writeLong(timestamp);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            revenue = in.readDouble();
            impressions = in.readLong();
            clicks = in.readLong();
            timestamp = in.readLong();
        }
    }

    // =========================================================================
    // 3. Custom Natural Key Partitioner
    // =========================================================================
    /**
     * Routes records to reducers based solely on the Natural Key (Advertiser + ContentType + Region).
     * The timestamp is excluded so that all events for a given natural group route to the exact same reducer.
     */
    public static class RevenueNaturalKeyPartitioner
            extends Partitioner<RevenueCompositeKey, RevenueEventWritable> {

        @Override
        public int getPartition(RevenueCompositeKey key, RevenueEventWritable value, int numPartitions) {
            if (numPartitions == 0) return 0;
            // Build partition hash on natural dimensions only
            int hash = 17;
            hash = 31 * hash + (key.getAdvertiserId() != null ? key.getAdvertiserId().hashCode() : 0);
            hash = 31 * hash + (key.getContentType() != null ? key.getContentType().hashCode() : 0);
            hash = 31 * hash + (key.getRegion() != null ? key.getRegion().hashCode() : 0);

            return Math.abs(hash % numPartitions);
        }
    }

    // =========================================================================
    // 4. Custom Key Sort Comparator (Composite Key Sorting)
    // =========================================================================
    /**
     * Controls the sort order of keys before passing to Reducer.
     * Sorts Natural Key ASC, then Timestamp ASC.
     */
    public static class RevenueCompositeKeyComparator extends WritableComparator {

        protected RevenueCompositeKeyComparator() {
            super(RevenueCompositeKey.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable w1, WritableComparable w2) {
            RevenueCompositeKey k1 = (RevenueCompositeKey) w1;
            RevenueCompositeKey k2 = (RevenueCompositeKey) w2;
            return k1.compareTo(k2);
        }
    }

    // =========================================================================
    // 5. Custom Grouping Comparator (Natural Key Grouping)
    // =========================================================================
    /**
     * Dictates how the Reducer groups incoming keys into a single reduce() invocation.
     * Evaluates ONLY the natural key components (Advertiser + ContentType + Region).
     * Because the Sort Comparator already sorted them chronologically, the Reducer
     * will receive all records for the natural key in strict chronological sequence!
     */
    public static class RevenueNaturalKeyGroupingComparator extends WritableComparator {

        protected RevenueNaturalKeyGroupingComparator() {
            super(RevenueCompositeKey.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable w1, WritableComparable w2) {
            RevenueCompositeKey k1 = (RevenueCompositeKey) w1;
            RevenueCompositeKey k2 = (RevenueCompositeKey) w2;

            int cmp = k1.getAdvertiserId().compareTo(k2.getAdvertiserId());
            if (cmp != 0) return cmp;

            cmp = k1.getContentType().compareTo(k2.getContentType());
            if (cmp != 0) return cmp;

            return k1.getRegion().compareTo(k2.getRegion());
        }
    }

    // =========================================================================
    // 6. Mapper Implementation
    // =========================================================================
    public static class RevenueMapper
            extends Mapper<LongWritable, Text, RevenueCompositeKey, RevenueEventWritable> {

        private final RevenueCompositeKey outKey = new RevenueCompositeKey();
        private final RevenueEventWritable outValue = new RevenueEventWritable();

        // Expected input format (TSV or CSV):
        // 0: impression_id
        // 1: ad_id
        // 2: advertiser_id
        // 3: campaign_id
        // 4: content_type (e.g. MOVIE, SERIES, LIVE)
        // 5: region (e.g. US_EAST, EMEA_CENTRAL, APAC_NORTH)
        // 6: timestamp (epoch ms or ISO string)
        // 7: revenue / clearing_price (e.g. 0.0350)
        // 8: clicks (0 or 1)
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(RevenueCounters.TOTAL_INPUT_EVENTS).increment(1);

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }

            String[] fields = line.split("\t");
            if (fields.length < 8) {
                fields = line.split(",");
                if (fields.length < 8) {
                    context.getCounter(RevenueCounters.CORRUPT_FORMAT_RECORDS).increment(1);
                    return;
                }
            }

            try {
                String advertiserId = fields[2].trim();
                String contentType = fields[4].trim();
                String region = fields[5].trim();
                String timeStr = fields[6].trim();
                String revenueStr = fields[7].trim();

                if (advertiserId.isEmpty() || contentType.isEmpty() || region.isEmpty()) {
                    context.getCounter(RevenueCounters.CORRUPT_FORMAT_RECORDS).increment(1);
                    return;
                }

                long timestampMs;
                if (timeStr.matches("\\d+")) {
                    timestampMs = Long.parseLong(timeStr);
                } else {
                    // Try parsing standard ISO-8601 date
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    Date d = sdf.parse(timeStr.replace("Z", ""));
                    timestampMs = d.getTime();
                }

                double revenue = Double.parseDouble(revenueStr);
                if (revenue < 0.0) {
                    context.getCounter(RevenueCounters.INVALID_PRICE_EVENTS).increment(1);
                    return;
                }

                long clicks = 0L;
                if (fields.length > 8 && !fields[8].trim().isEmpty()) {
                    clicks = Long.parseLong(fields[8].trim());
                }

                if (revenue >= 1.0) {
                    context.getCounter(RevenueCounters.HIGH_VALUE_IMPRESSIONS).increment(1);
                }

                outKey.set(advertiserId.toUpperCase(), contentType.toUpperCase(), region.toUpperCase(), timestampMs);
                outValue.set(revenue, 1L, clicks, timestampMs);

                context.write(outKey, outValue);
                context.getCounter(RevenueCounters.VALID_AD_EVENTS).increment(1);

            } catch (Exception ex) {
                context.getCounter(RevenueCounters.CORRUPT_FORMAT_RECORDS).increment(1);
            }
        }
    }

    // =========================================================================
    // 7. Reducer Implementation (Chronological Aggregation & Secondary Sort Consumer)
    // =========================================================================
    public static class RevenueReducer
            extends Reducer<RevenueCompositeKey, RevenueEventWritable, RevenueCompositeKey, Text> {

        private final Text resultText = new Text();
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        protected void setup(Context context) {
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        @Override
        protected void reduce(RevenueCompositeKey key, Iterable<RevenueEventWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalRevenue = 0.0;
            long totalImpressions = 0L;
            long totalClicks = 0L;

            long firstEventTime = -1L;
            long lastEventTime = -1L;

            // Tracking revenue pacing across time windows
            double peakHourRevenue = 0.0;
            long currentHourBucket = -1L;
            double currentHourRevenue = 0.0;

            // Because secondary sort guarantees chronological delivery:
            for (RevenueEventWritable event : values) {
                long eventTs = event.getTimestamp();

                if (firstEventTime == -1L) {
                    firstEventTime = eventTs;
                }
                lastEventTime = eventTs;

                double rev = event.getRevenue();
                long imp = event.getImpressions();
                long clk = event.getClicks();

                totalRevenue += rev;
                totalImpressions += imp;
                totalClicks += clk;

                // Hour bucket calculation (ms to hour)
                long hourBucket = eventTs / 3600000L;
                if (hourBucket == currentHourBucket) {
                    currentHourRevenue += rev;
                } else {
                    if (currentHourRevenue > peakHourRevenue) {
                        peakHourRevenue = currentHourRevenue;
                    }
                    currentHourBucket = hourBucket;
                    currentHourRevenue = rev;
                }
            }

            if (currentHourRevenue > peakHourRevenue) {
                peakHourRevenue = currentHourRevenue;
            }

            // eCPM = (Total Revenue / Total Impressions) * 1000
            double ecpm = (totalImpressions > 0) ? (totalRevenue / totalImpressions) * 1000.0 : 0.0;
            // CTR = (Total Clicks / Total Impressions) * 100.0
            double ctr = (totalImpressions > 0) ? ((double) totalClicks / totalImpressions) * 100.0 : 0.0;

            String startTimeStr = (firstEventTime > 0) ? dateFormat.format(new Date(firstEventTime)) : "N/A";
            String endTimeStr = (lastEventTime > 0) ? dateFormat.format(new Date(lastEventTime)) : "N/A";

            // Output format:
            // totalRevenue \t totalImpressions \t totalClicks \t eCPM \t CTR% \t peakHourRev \t firstEvent \t lastEvent
            String summary = String.format("%.4f\t%d\t%d\t%.4f\t%.3f%%\t%.4f\t%s\t%s",
                    totalRevenue,
                    totalImpressions,
                    totalClicks,
                    ecpm,
                    ctr,
                    peakHourRevenue,
                    startTimeStr,
                    endTimeStr);

            resultText.set(summary);
            context.write(key, resultText);
        }
    }

    // =========================================================================
    // 8. Tool Configuration & Job Runner
    // =========================================================================
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: RevenueAnalysisDriver [generic options] <input_path> <output_path> [num_reducers]");
            System.err.println("  input_path   : HDFS directory containing ad impression / revenue events");
            System.err.println("  output_path  : Destination path in HDFS for monetization reports");
            System.err.println("  num_reducers : (Optional) Number of reducers (default: 8)");
            return -1;
        }

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path(otherArgs[1]);
        int numReducers = (otherArgs.length >= 3) ? Integer.parseInt(otherArgs[2]) : 8;

        // Enable map output compression
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "MediaWarehouse_AdRevenue_SecondarySort_Analysis");
        job.setJarByClass(RevenueAnalysisDriver.class);

        // Input / Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Mapper & Reducer
        job.setMapperClass(RevenueMapper.class);
        job.setReducerClass(RevenueReducer.class);

        // Secondary Sort Wiring:
        // 1. Partitioner by Natural Key
        job.setPartitionerClass(RevenueNaturalKeyPartitioner.class);
        // 2. Sort Comparator by Composite Key (Natural Key + Timestamp)
        job.setSortComparatorClass(RevenueCompositeKeyComparator.class);
        // 3. Grouping Comparator by Natural Key only
        job.setGroupingComparatorClass(RevenueNaturalKeyGroupingComparator.class);

        // Map Output Key and Value
        job.setMapOutputKeyClass(RevenueCompositeKey.class);
        job.setMapOutputValueClass(RevenueEventWritable.class);

        // Reducer Output Key and Value
        job.setOutputKeyClass(RevenueCompositeKey.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(numReducers);

        LOG.info("Submitting Secondary-Sort Job [RevenueAnalysisDriver]...");
        LOG.info("Input Path   : {}", inputPath);
        LOG.info("Output Path  : {}", outputPath);
        LOG.info("Reducers     : {}", numReducers);

        boolean ok = job.waitForCompletion(true);

        if (ok) {
            LOG.info("Revenue analysis job finished successfully. Metric counters:");
            for (RevenueCounters c : RevenueCounters.values()) {
                Counter counter = job.getCounters().findCounter(c);
                LOG.info("  {}: {}", counter.getDisplayName(), counter.getValue());
            }
            return 0;
        } else {
            LOG.error("Revenue analysis job failed.");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RevenueAnalysisDriver(), args);
        System.exit(res);
    }
}
