package com.derivative;

import org.joda.time.DateTime;
import org.joda.time.Days;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;


public class OptionCalculationUtil {


    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static DateTime dateNow = new DateTime(Calendar.getInstance().getTime());
    private static JedisPool jedisPool = null;



    public static void writeToRedis (ArrayList<String> priceDataList) {


        Jedis jedis = JedisConnectionsManager.getJedis(2);
        jedis.mset(priceDataList.toArray(new String[]{}));
        jedis.close();

    }

    public static ArrayList<Double> getStockPriceAndVol (String symbol) {

        double stockPrice = 0.0;
        double volatility = 0.0;
        Jedis jedis  = JedisConnectionsManager.getJedis(3);
        String priceValue = jedis.hget(symbol,"spot_price");
        if(priceValue == null){
            System.out.println("No price for this symbol :"+symbol);
            priceValue =  "0.0";
        }
        String vol = jedis.hget(symbol,"vol");
        if(vol == null){
            System.out.println("No vol for this symbol :"+symbol);
            vol =  "0.0";
        }
        try{
            stockPrice = Double.valueOf(priceValue);

        }
        catch(NumberFormatException n){
            System.out.println("Bad value of Stock Price : "+ priceValue + " for symbol:"+symbol );
            stockPrice = 0.0;
        }

        try{
            volatility = Double.valueOf(vol);

        }
        catch(NumberFormatException n){
            System.out.println("Bad value of Vol : "+ priceValue + " for symbol:"+symbol );
            volatility = 0.0;
        }

        ArrayList<Double> priceVol = new ArrayList<Double>(2);
        priceVol.add(stockPrice);
        priceVol.add(volatility);

        jedis.close();

        return priceVol;
    }

    public static double getInterestRate (String symbol) {

        double iRate = 0.0;
        String interestRate = null;
        try{
            Jedis jedis = JedisConnectionsManager.getJedis(4);
            interestRate =  jedis.hget(symbol, "interest_rate");
            jedis.close();
            if(interestRate == null){
                System.out.println("No Interest Rate ");
                return iRate;
            }

            iRate = Double.valueOf(interestRate);



        }
        catch(NumberFormatException n){
            System.out.println("Bad value of interestRate : "+ interestRate + " for symbol:"+symbol );
            iRate = 0.0;
        }

        return iRate;
    }


    public static double getTimeToExpiry(String dateInString) {

        double timeToExpiry = 0.0;
        try {


            Date date1 = format.parse(dateInString);
            DateTime dt1 = new DateTime(date1);


            int daysInt =  Days.daysBetween(dateNow, dt1).getDays();
            if(daysInt < 0)
                return timeToExpiry;
            timeToExpiry = (double)daysInt/365;
           // BigDecimal bigDecimal = new BigDecimal(days);
            //bigDecimal.
            //timeToExpiry = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            //System.out.println(timeToExpiry);


        } catch (Exception e) {
            System.out.println("#################"+e.toString());

        }

        return  timeToExpiry;
    }





}
