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

public class LAJsonToTableRow extends DoFn<String, TableRow> {
    private final Logger LOG = LoggerFactory.getLogger(LAJsonToTableRow.class);

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        LOG.debug("PIPELINE STEP2: {}", c.element());
        DocumentContext context = JsonPath.parse(c.element());

        TableRow tr = new TableRow();
        tr.set("order_number", context.read("$.after.fields.order_number", Integer.class));
        tr.set("purchaser", context.read("$.after.fields.purchaser", Integer.class));
        tr.set("quantity", context.read("$.after.fields.quantity", Integer.class));
        tr.set("product_id", context.read("$.after.fields.product_id", Integer.class));
        long epochMillis = context.read("$.after.fields.order_datetime_pst", Long.class);
        Date order_date = new Date(epochMillis);
        // To UTC
        TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(order_date);
        calendar.setTimeZone(timeZone);
        calendar.add(Calendar.HOUR, -8); // GMT-8
        SimpleDateFormat formatterWithTimeZone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatterWithTimeZone.setTimeZone(TimeZone.getTimeZone("UTC"));

        tr.set("order_datetime_utc", formatterWithTimeZone.format(order_date));
        tr.set("source", "la_online_orders");

        c.output(tr);
    }
}
