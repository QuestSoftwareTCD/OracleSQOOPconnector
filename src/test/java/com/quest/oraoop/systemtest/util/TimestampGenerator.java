/**
 *   Copyright 2011 Quest Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.quest.oraoop.systemtest.util;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Generates test data for Oracle DATE columns. Generated Timestamps are between 4711BC and 9999AD.
 * 
 * @author phall
 */
public class TimestampGenerator extends TestDataGenerator<Timestamp> {
	private static final int nanosDigits = 9;
	private static final int minYear = -4711;
	private static final int maxYear = 9999;
	private final int precision;
	private final Calendar cal = Calendar.getInstance();

	/**
	 * Create a TimestampGenerator that will generate Timestamps with a given precision.
	 * 
	 * @param precision
	 *            Number of decimal digits after the decimal point in the seconds of generated Timestamps.
	 */
	public TimestampGenerator(int precision) {
		this.precision = precision;
	}

	@Override
	public Timestamp next() {
		cal.clear();
		cal.set(Calendar.YEAR, minYear + rng.nextInt(maxYear - minYear + 1));
		cal.set(Calendar.DAY_OF_YEAR, 1 + rng.nextInt(cal.getActualMaximum(Calendar.DAY_OF_YEAR)));
		cal.set(Calendar.HOUR_OF_DAY, rng.nextInt(24));
		cal.set(Calendar.MINUTE, rng.nextInt(60));
		cal.set(Calendar.SECOND, rng.nextInt(cal.getActualMaximum(Calendar.SECOND)));
		// Workaround for oracle jdbc bugs related to BC leap years
		if (cal.get(Calendar.ERA) == GregorianCalendar.BC && cal.get(Calendar.MONTH) == 1
				&& cal.get(Calendar.DAY_OF_MONTH) >= 28) {
			return next();
		}
		Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
		if (precision > 0) {
			int nanos = rng.nextInt((int) Math.pow(10, precision));
			timestamp.setNanos(nanos * (int) Math.pow(10, nanosDigits - precision));
		}
		return timestamp;
	}

}
