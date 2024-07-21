package com.twq.prepaser;

public class WebLogPreParser {
    public static  PreParsedLog parse(String line){
        if (line.startsWith("#")){
            return null;
        }else {
            PreParsedLog preParsedLog = new PreParsedLog();
            String[] temps = line.split(" ");
            //第一个字段时间
            preParsedLog.setServerTime(temps[0] + " " +temps[1]);
            //第二个字段
            preParsedLog.setServerIp(temps[2]);
            //第三个字符
            preParsedLog.setMethod(temps[3]);
            preParsedLog.setUriStem(temps[4]);
            //很长
            String queryString = temps[5];
            preParsedLog.setQueryString(queryString);
            String[] queryStrTemps = queryString.split("&");
            String command = queryStrTemps[1].split("=")[1];
            preParsedLog.setCommand(command);
            String profileIdStr = queryStrTemps[2].split("=")[1];
            preParsedLog.setProfileId(getProfileId(profileIdStr));
            preParsedLog.setServerPort(Integer.parseInt(temps[6]));
            preParsedLog.setClientIp(temps[8]);
            preParsedLog.setUserAgent(temps[9].replace("+"," "));
            String tempTime = preParsedLog.getServerTime().replace("-", "");
            preParsedLog.setDay(Integer.parseInt(tempTime.substring(0,8)));
            preParsedLog.setMonth(Integer.parseInt(tempTime.substring(0,6)));
            preParsedLog.setYear(Integer.parseInt(tempTime.substring(0,4)));

            return preParsedLog;
        }
    }

    private static int getProfileId(String profileIdStr){
        return Integer.valueOf(profileIdStr.substring(profileIdStr.indexOf("-")+1));
    }
}
