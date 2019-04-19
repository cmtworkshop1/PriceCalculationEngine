package com.derivative;

import org.joda.time.DateTime;
import org.joda.time.Days;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class OptionCalculationUtil {

    private static final Jedis jedis = new Jedis("localhost");
    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");



    public static void writeToRedis (String opt, double cp, double pp) {

        String d1 = jedis.hget(opt,"CallPrice");
        String d2 = jedis.hget(opt,"PutPrice");
        System.out.println("Old Call price : "+d1);
        System.out.println("Old Old price : "+d2);


        HashMap hmPrice = new HashMap();
        hmPrice.put("CallPrice",Double.toString(cp));
        hmPrice.put("PutPrice",Double.toString(pp));
        jedis.hset(opt, hmPrice);


        System.out.println("New Call price : "+cp);
        System.out.println("New Put price : "+pp);


    }

    public static double getStockPrice (String symbol) {

        String priceValue = jedis.hget(symbol,"price");
        if(priceValue == null){
            System.out.println("No price for this symbol :"+symbol);
            return 0.0;
        }
        return Double.valueOf(priceValue);
    }

    public static double getVolatility (String symbol) {

        String vol = jedis.hget(symbol,"vol");
        if(vol == null){
            System.out.println("No vol for this symbol :"+symbol);
            return 0.0;
        }
        return Double.valueOf(vol);
    }


    public static double getTimeToExpiry(String dateInString) {

        double timeToExpiry = 0.0;
        try {
            Date date1 = format.parse(dateInString);
            DateTime dt1 = new DateTime(date1);


            Date date2 = Calendar.getInstance().getTime();
            DateTime dt2 = new DateTime(date2);

            int daysInt =  Days.daysBetween(dt2, dt1).getDays();
            double days = (double)daysInt/365;
            BigDecimal bigDecimal = new BigDecimal(days);
            timeToExpiry = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            System.out.println(timeToExpiry);


        } catch (Exception e) {
            System.out.println(e.toString());

        }

        return  timeToExpiry;
    }





}
