package cn.edu.tsinghua.rafaelzig.datastructure;

// Created by Zig on 29/04/2016

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

/**
 * TODO
 *
 * @author Rafael da Silva Costa - 2015280364
 * @version 1.0
 */
public class Point implements Serializable
{
	private static final long serialVersionUID = 4412827817402129848L;

	private static final String[][] DICTIONARY =
			{
					{"icmp", "tcp", "udp"},
					{"aol", "auth", "bgp", "courier", "csnet_ns", "ctf", "daytime", "discard", "domain", "domain_u",
							"echo", "eco_i", "ecr_i", "efs", "exec", "finger", "ftp", "ftp_data", "gopher", "harvest",
							"hostnames", "http", "http_2784", "http_443", "http_8001", "icmp", "imap4", "IRC",
							"iso_tsap", "klogin", "kshell", "ldap", "link", "login", "mtp", "name", "netbios_dgm",
							"netbios_ns", "netbios_ssn", "netstat", "nnsp", "nntp", "ntp_u", "other", "pm_dump",
							"pop_2", "pop_3", "printer", "private", "remote_job", "rje", "shell", "smtp", "sql_net",
							"ssh", "sunrpc", "supdup", "systat", "telnet", "tftp_u", "tim_i", "time", "urh_i", "urp_i",
							"uucp", "uucp_path", "vmnet", "whois", "X11", "Z39_50"},
					{"OTH", "REJ", "RSTO", "RSTOS0", "RSTR", "S0", "S1", "S2", "S3", "SF", "SH"}
			};

	private static final List<Map<String, Float>> MAPPINGS         = new ArrayList<>(DICTIONARY.length);
	private static final String[][]               REVERSE_MAPPINGS = new String[DICTIONARY.length][];
	private static final DecimalFormat            df               = new DecimalFormat("#.##");

	static
	{
		for (int i = 0; i < DICTIONARY.length; ++i)
		{
			Map<String, Float> mapping = new HashMap<>(DICTIONARY[i].length);
			String[] reverseMapping = new String[DICTIONARY[i].length];

			for (int j = 0; j < DICTIONARY[i].length; ++j)
			{
				mapping.put(DICTIONARY[i][j], (float) j);
				reverseMapping[j] = DICTIONARY[i][j];
			}

			MAPPINGS.add(mapping);
			REVERSE_MAPPINGS[i] = reverseMapping;
		}
	}

	protected static final int     FEATURES  = 41;
	private static final   String  DELIMITER = ",";
	private final          float[] data      = new float[FEATURES];
	private                int     label     = -1;
	private                float   magnitude = 0.0f;

	Point()
	{
		super();
	}

	public Point(Point first, Point second)
	{
		this();

		for (int i = 0; i < FEATURES; ++i)
		{
			data[i] = first.data[i] + second.data[i];
		}
	}

	public Point(String csv)
	{
		this();
		parseData(csv);
		normalizeData();
	}

	private Point(int label, float magnitude)
	{
		this();
		this.label = label;
		this.magnitude = magnitude;
	}

	Point(int label, Point another)
	{
		this(label, another.magnitude);
		System.arraycopy(another.data, 0, data, 0, FEATURES);
	}

	public Point(Point another)
	{
		this(another.label, another);
	}

	public Point(Point another, Iterable<Point> centroids)
	{
		this(another.label, another);
		assignCluster(centroids);
	}

	public void setLabel(int label)
	{
		this.label = label;
	}

	public int getLabel()
	{
		return label;
	}

	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();

		for (int col = 0; col < FEATURES; ++col)
		{
			float value = data[col] * magnitude;

			if (isIntegerColumn(col))
			{
				int rounded = Math.round(value);
				builder.append(isStringColumn(col) ? REVERSE_MAPPINGS[col - 1][rounded] : rounded);
			}
			else
			{
				builder.append(df.format(value));
			}

			builder.append(DELIMITER);
		}

		return builder.append(label).toString();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null || getClass() != obj.getClass())
		{
			return false;
		}

		Point point = (Point) obj;

		return Double.compare(point.magnitude, magnitude) == 0 &&
				label == point.label && Arrays.equals(data, point.data);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(data, magnitude, label);
	}

	private void parseData(String csv)
	{
		double sum = 0.0;
		StringTokenizer tokenizer = new StringTokenizer(csv, DELIMITER);

		// Parse values from stream and sum them to calculate vector magnitude
		for (int col = 0; col < FEATURES; ++col)
		{
			String token = tokenizer.nextToken();
			data[col] = isStringColumn(col) ? MAPPINGS.get(col - 1).get(token) : Float.parseFloat(token);
			sum += Math.pow(data[col], 2.0);
		}

		magnitude = (float) Math.sqrt(sum);
	}

	private void assignCluster(Iterable<Point> centroids)
	{
		float closest = Float.MAX_VALUE;

		// For each centroid
		for (Point centroid : centroids)
		{
			// Calculate the distance of point to current centroid
			float distance = calculateDistance(centroid);

			if (distance < closest)
			{
				label = centroid.label;
				closest = distance;
			}
		}
	}

	private void normalizeData()
	{
		if (magnitude > 0)
		{
			for (int i = 0; i < FEATURES; i++)
			{
				data[i] /= magnitude;
			}
		}
	}

	/**
	 * Calculate the Euclidean Distance to another point.
	 *
	 * @param another
	 * @return
	 */
	float calculateDistance(Point another)
	{
		float distance = 0.0f;

		for (int col = 0; col < FEATURES; ++col)
		{
			distance += Math.pow(another.data[col] - data[col], 2.0);
		}

		return (float) Math.sqrt(distance);
	}

	/**
	 * Move centroid to the mean of the sum vector
	 *
	 * @param sumVector
	 * @param clusterSize
	 */
	public void move(Point sumVector, long clusterSize)
	{
		for (int i = 0; i < FEATURES; ++i)
		{
			BigDecimal bd = new BigDecimal(sumVector.data[i] / clusterSize);
			bd = bd.setScale(7, RoundingMode.HALF_DOWN);
			data[i] = bd.floatValue();
		}
	}

	private static boolean isIntegerColumn(int col)
	{
		return col <= 23 || col == 31 || col == 32;
	}

	private static boolean isStringColumn(int col)
	{
		return col >= 1 && col <= 3;
	}
}