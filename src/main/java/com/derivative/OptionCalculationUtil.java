package com.derivative;

import org.joda.time.DateTime;
import org.joda.time.Days;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class OptionCalculationUtil {

    private static Jedis jedis = null;
    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");



    public static void writeToRedis (String opt, double cp) {

        //String d1 = jedis.hget(opt,"CallPrice");
        //String d2 = jedis.hget(opt,"PutPrice");
        //System.out.println("Old Call price : "+d1);
        //System.out.println("Old Old price : "+d2);


        //HashMap hmPrice = new HashMap();
        //hmPrice.put("CallPrice",Double.toString(cp));
        //hmPrice.put("PutPrice",Double.toString(pp));
        jedis.select(2);
        jedis.set(opt, Double.toString(cp));


        System.out.println("New Call price : "+cp);
       // System.out.println("New Put price : "+pp);


    }

    public static double getStockPrice (String symbol) {

        jedis.select(3);
        String priceValue = jedis.hget(symbol,"spot_price");
        if(priceValue == null){
            System.out.println("No price for this symbol :"+symbol);
            return 0.0;
        }
        return Double.valueOf(priceValue);
    }

    public static double getInterestRate () {

        jedis.select(4);
        String interestRate = jedis.get("Interest_rate");
        if(interestRate == null){
            System.out.println("No Interest Rate ");
            return 0.0;
        }
        return Double.valueOf(interestRate);
    }

    public static double getVolatility (String symbol) {

        jedis.select(3);
        String vol = jedis.hget(symbol,"vol");
        if(vol == null){
            System.out.println("No vol for this symbol :"+symbol);
            return 0.0;
        }
        return Double.valueOf(vol);
    }


    public static void setJedis(String url ){

        //new Jedis("localhost");

        jedis = new Jedis(url);





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
