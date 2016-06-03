/**
 * Processor of TSQL2 on a Relational Database System
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.
 * It is also available through the world-wide-web at this URL:
 * http://www.opensource.org/licenses/bsd-license.php
 *
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license http://www.opensource.org/licenses/bsd-license.php New BSD License
 */
package cz.vutbr.fit.tsql2lib;

import java.util.Calendar;

/**
 * Utility methods for TSQL2 library.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class Utils implements Constants {

    /**
     * Enclose identifier in quotes for current DBMS. This should be used for
     * column or table names for statements.
     *
     * @param str String to enclose
     * @return Identifier enclosed in quotes for current DBMS
     */
    public static String quote(String str) {
        if ((str.length() == 0) || (str.charAt(0) != Settings.QUOTE)) {
            return Settings.QUOTE + str + Settings.QUOTE;
        } else {
            return str;
        }
    }

    /**
     * Strip quotes from identifier
     *
     * @param str String to unquote
     * @return Identifier without leading and trailing quotes
     */
    public static String unquote(String str) {
        if (str.length() == 0) {
            return str;
        }
        if ((str.charAt(0) == Settings.QUOTE) && (str.charAt(str.length() - 1) == Settings.QUOTE)) {
            str = str.substring(1, str.length() - 1);
        }
        return str;
    }

    /**
     * Enclose string in quotes for current DBMS. This should be used for plain
     * strings for statements.
     *
     * @param str String to enclose
     * @return String enclosed in quotes for current DBMS
     */
    public static String quoteString(String str) {
        if ((str.length() == 0) || (str.charAt(0) != Settings.STRING_QUOTE)) {
            return Settings.STRING_QUOTE + str + Settings.STRING_QUOTE;
        } else {
            return str;
        }
    }

    /**
     * Strip quotes from string
     *
     * @param str String to unquote
     * @return String without leading and trailing quotes
     */
    public static String unquoteString(String str) {
        if (str.length() == 0) {
            return str;
        }
        if ((str.charAt(0) == Settings.STRING_QUOTE) && (str.charAt(str.length() - 1) == Settings.STRING_QUOTE)) {
            str = str.substring(1, str.length() - 1);
        }
        return str;
    }

    /**
     * Convert time from unix timestamp to string representation
     *
     * @param seconds Time in seconds in Unix timestamp
     * @return YYYY-MM-DD HH:MIN:SS
     */
    public static String timeToString(long seconds) {
        return timeToString(seconds, DateTimeScale.SECOND);
    }

    /**
     * Convert time from unix timestamp to string representation with specified
     * resulting scale.
     *
     * @param seconds Time in seconds in Unix timestamp
     * @param scale Scale of resulting time
     * @return Depending on scale
     */
    public static String timeToString(long seconds, DateTimeScale scale) {
        if (seconds == FOREVER) {
            return "NOW";
        }

        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(seconds * 1000);

        String result = "";
        switch (scale) {
            case YEAR:
                result = String.format("%04d",
                        c.get(Calendar.YEAR));
                break;
            case MONTH:
                result = String.format("%04d-%02d",
                        c.get(Calendar.YEAR),
                        c.get(Calendar.MONTH) + 1);
                break;
            case DAY:
                result = String.format("%04d-%02d-%02d",
                        c.get(Calendar.YEAR),
                        c.get(Calendar.MONTH) + 1,
                        c.get(Calendar.DAY_OF_MONTH));
                break;
            case HOUR:
                result = String.format("%04d-%02d-%02d %02d:%02d",
                        c.get(Calendar.YEAR),
                        c.get(Calendar.MONTH) + 1,
                        c.get(Calendar.DAY_OF_MONTH),
                        c.get(Calendar.HOUR_OF_DAY),
                        0);
                break;
            case MINUTE:
                result = String.format("%04d-%02d-%02d %02d:%02d",
                        c.get(Calendar.YEAR),
                        c.get(Calendar.MONTH) + 1,
                        c.get(Calendar.DAY_OF_MONTH),
                        c.get(Calendar.HOUR_OF_DAY),
                        c.get(Calendar.MINUTE));
                break;
            case SECOND:
                result = String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        c.get(Calendar.YEAR),
                        c.get(Calendar.MONTH) + 1,
                        c.get(Calendar.DAY_OF_MONTH),
                        c.get(Calendar.HOUR_OF_DAY),
                        c.get(Calendar.MINUTE),
                        c.get(Calendar.SECOND));
                break;
        }

        return result;
    }

    /**
     * Convert date string to unix timestamp
     *
     * @param date String representing date in format YYYY[-MM[-DD[
     * HH[:MM[:SS]]]]]
     * @return Number of seconds in unix timestamp format
     */
    public static long dateToTimestamp(String date) {
        if (date.startsWith("'")) {
            date = date.substring(1);
        }
        if (date.endsWith("'")) {
            date = date.substring(0, date.length() - 1);
        }

        Calendar cal = Calendar.getInstance();
        // init to UTC beginning
        cal.set(1970, 1, 1, 0, 0, 0);

        String[] dtFields = date.split(" ");
        String[] dateFields = dtFields[0].split("-");

        if (dateFields.length >= 1) {
            if (dateFields[0].equalsIgnoreCase("FOREVER")) {
                return FOREVER;
            }
            cal.set(Calendar.YEAR, Integer.parseInt(dateFields[0]));
        }
        if (dateFields.length >= 2) {
            cal.set(Calendar.MONTH, Integer.parseInt(dateFields[1]) - 1);
        }
        if (dateFields.length >= 3) {
            cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(dateFields[2]));
        }

        if (dtFields.length == 2) {
            String[] timeFields = dtFields[1].split(":");

            if (timeFields.length >= 1) {
                cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(timeFields[0]));
            }
            if (timeFields.length >= 2) {
                cal.set(Calendar.MINUTE, Integer.parseInt(timeFields[1]));
            }
            if (timeFields.length >= 3) {
                cal.set(Calendar.SECOND, Integer.parseInt(timeFields[2]));
            }
        }
        return cal.getTimeInMillis() / 1000;
    }

    /**
     * Get current time in unix timestamp seconds.
     *
     * @return Unixc timestamp for current time
     */
    public static long getCurrentTime() {
        return System.currentTimeMillis() / 1000;
    }
}
