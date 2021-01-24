package com.google.cloud.teleport.cdc;

import com.google.api.services.bigquery.model.TableRow;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class ChicagoJsonToTableRow extends DoFn<String, TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(ChicagoJsonToTableRow.class);

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        LOG.debug("PIPELINE STEP2: {}", c.element());
        DocumentContext context = JsonPath.parse(c.element());

        TableRow tr = new TableRow();
        tr.set("order_number", context.read("$.after.fields.order_number", Integer.class));
        tr.set("purchaser", context.read("$.after.fields.purchaser", Integer.class));
        tr.set("quantity", context.read("$.after.fields.quantity", Integer.class));
        tr.set("product_id", context.read("$.after.fields.product_id", Integer.class));
        // Get date
        int epochDay = context.read("$.after.fields.order_date", Integer.class);
        long epochMillis = TimeUnit.DAYS.toMillis(epochDay);
        Date order_date = new Date(epochMillis);
        // Get time
        long microsecs = context.read("$.after.fields.order_time_cst", Long.class);
        long millis = TimeUnit.MILLISECONDS.convert(microsecs, TimeUnit.MICROSECONDS);
        Date order_time = new Date(millis);
        Calendar calendar_time = Calendar.getInstance();
        calendar_time.setTime(order_time);
        // To UTC
        TimeZone timeZone = TimeZone.getTimeZone("America/Chicago");
        Calendar calendar_date = Calendar.getInstance();
        calendar_date.setTime(order_date);
        calendar_date.add(Calendar.HOUR, calendar_time.get(Calendar.HOUR));
        calendar_date.add(Calendar.MINUTE, calendar_time.get(Calendar.MINUTE));
        calendar_date.add(Calendar.SECOND, calendar_time.get(Calendar.SECOND));
        calendar_date.add(Calendar.HOUR, -8); // GMT-8
        calendar_date.setTimeZone(timeZone);
        SimpleDateFormat formatterWithTimeZone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatterWithTimeZone.setTimeZone(TimeZone.getTimeZone("UTC"));

        tr.set("order_datetime_utc", formatterWithTimeZone.format(calendar_date.getTime()));
        tr.set("source", "chicago_online_orders");

        c.output(tr);
    }
}
