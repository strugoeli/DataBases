package Join;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ExternalMemoryImpl extends IExternalMemory {

	private static final String BY_SPACE = " ";
	private static final int BLOCK_SIZE = 4 * 1024;
	private static final int M = 10000;


	@Override
	public void sort(String in, String out, String tmpPath) {
		try {
			tmpPath+='/';
			long fileSize = (new File(in).length()) * 2;
			int BR = (int) Math.ceil((double) fileSize / BLOCK_SIZE);
			createSortedSequences(in, tmpPath, BR);
			merge(BR, out, tmpPath);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void join(String in1, String in2, String out, String tmpPath) {
		try {
			BufferedReader buf1 = getBufferedReader(in1);
			BufferedReader buf2 = getBufferedReader(in2);
			FileWriter writer = new FileWriter(out);
			String Tr = buf1.readLine(), Ts = buf2.readLine(), Gs = Ts;

			while (Tr != null && Gs != null) {
				while (Tr != null && compareKeys(Tr, Gs) < 0) {
					Tr = buf1.readLine();
				}
				while (Gs != null && Tr != null && compareKeys(Gs, Tr) < 0) {
					Gs = buf2.readLine();
				}
				while (Tr != null && Gs != null && compareKeys(Tr, Gs) == 0) {
					Ts = Gs;
					while (Ts.compareTo(Gs)!=0)
					{
						Ts= buf2.readLine();
					}
					while (Ts != null && compareKeys(Tr, Ts) == 0) {
						writer.write(Tr + Ts.substring(10) + '\n');
						Ts = buf2.readLine();
					}
					Tr = buf1.readLine();
				}
				Gs=Ts;
			}
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void select(String in, String out, String substrSelect, String tmpPath) {

		try {
			BufferedReader bufferedReader = getBufferedReader(in);
			FileWriter writer = new FileWriter(out);
			String currLine;
			while ((currLine = bufferedReader.readLine()) != null) {
				if (currLine.split(BY_SPACE)[0].contains(substrSelect)) {
					writer.write(currLine + '\n');
				}
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void joinAndSelectEfficiently(String in1, String in2, String out,
										 String substrSelect, String tmpPath) {

		String outFileName = new File(out).getName();
		String tmpFileName1 = getTmpName(outFileName,1);
		String tmpFileName2 = getTmpName(outFileName,2);
		String tmpOut1 = Paths.get(tmpPath, tmpFileName1).toString();
		String tmpOut2 = Paths.get(tmpPath, tmpFileName2).toString();
		this.select(in1, tmpOut1, substrSelect, tmpPath);
		this.select(in2, tmpOut2, substrSelect, tmpPath);

		sortAndJoin(tmpOut1, tmpOut2, out, tmpPath);
		try {
			Files.deleteIfExists(Paths.get(tmpPath, tmpOut1, tmpOut2));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return the name of the temp file of the given number.
	 */
	private String getTmpName(String outFileName ,int tempNum) {
		return outFileName.substring(0, outFileName.lastIndexOf('.'))
				+ "_intermed"+tempNum + outFileName.substring(outFileName.lastIndexOf('.'));
	}
	/**
	 *
	 * @param str1 line in file1 to compare
	 * @param str2 line in file2 to compare
	 * @return if str1 > str2, it returns positive number
	 * if str1 < str2, it returns negative number
	 * if str1 == str2, it returns 0
	 */
	private int compareKeys(String str1, String str2) {
		return str1.split(BY_SPACE)[0].compareTo(str2.split(BY_SPACE)[0]);
	}
	/**
	 *
	 * @param in the file (relation)
	 * @param tmpPath path for temporary directory
	 * @param BR the relation's block number
	 * @throws IOException for buffer exceptions
	 */
	private void createSortedSequences(String in, String tmpPath, int BR) throws IOException {
		int left = 0, right;
		List<String> buffer = new ArrayList<>();
		BufferedReader bufferedReader = getBufferedReader(in);
		FileWriter writer;
		int tempCounter = 0;

		while (left < BR) {
			right = Math.min(left + M, BR) - 1;
			for (int i = left; i < right + 1; i++) {
				getBlock(bufferedReader, buffer);
			}
			buffer.sort(new Comp());
			writer = new FileWriter(tmpPath + tempCounter + ".txt");
			for (String val : buffer) {
				writer.write(val);
			}
			tempCounter++;
			writer.close();
			buffer.clear();
			left += M;
		}
	}
	/**
	 *
	 * @param file file to read
	 * @return BufferedReader to read text from file
	 * @throws FileNotFoundException for file exception
	 */
	private BufferedReader getBufferedReader(String file) throws FileNotFoundException {
		return new BufferedReader(new FileReader(file), BLOCK_SIZE);
	}
	/**
	 *
	 * @param BR the relation's block number
	 * @param out the output file which will contain the sorted relation
	 * @param tmpPath contains the sorted sequences (BR/M)
	 * @throws IOException for file , BufferedReader and Map exceptions
	 */
	private void merge(int BR, String out, String tmpPath) throws IOException {
		int seqNum;
		int numOfSeq = (int) Math.ceil(1.0 * BR / M);
		String currLine, nextLine;
		BufferedReader[] p = new BufferedReader[numOfSeq + 1];
		FileWriter writer =new FileWriter(out);
		Map<String, Integer> map = new TreeMap<>();

		for (int i = 0; i < numOfSeq; i++)
		{
			p[i] = getBufferedReader(tmpPath + i + ".txt");
			currLine = p[i].readLine();
			if (currLine != null) {
				map.put(currLine, i);
			}
		}
		while (!map.isEmpty())
		{
			currLine = map.keySet().iterator().next();
			seqNum = map.get(currLine);
			map.remove(currLine);

			nextLine = p[seqNum].readLine();
			if (nextLine != null) {
				map.put(nextLine, seqNum);
			}
			currLine = (map.isEmpty()) ? currLine : currLine + '\n';
			writer.write(currLine);

		}
		writer.close();
		for (int i = 0; i < numOfSeq; i++)
		{
			Files.deleteIfExists(Paths.get(tmpPath + i + ".txt"));
		}
	}
	/**
	 *
	 * @param bufferedReader the file (relation) bufferedReader
	 * @param buffer the list which contains the relation data
	 * @throws IOException for the bufferedReader and List exceptions
	 */
	private void getBlock(BufferedReader bufferedReader, List<String> buffer) throws IOException {
		String CurrentLine;
		int curr_size = 0;

		while (2 * curr_size < BLOCK_SIZE) {
			CurrentLine = bufferedReader.readLine();
			if (CurrentLine == null) {
				break;
			}
			curr_size += CurrentLine.length();
			buffer.add(CurrentLine + '\n');
		}
	}

	public static class Comp implements java.util.Comparator<String> {
		/**
		 *
		 * @param s1 relation1 tuple
		 * @param s2 relation2 tuple
		 * @return if all columns are equal then it returns 0, else, (for the first different columns)
		 * if s1 > s2, it returns positive number
		 * if s1 < s2, it returns negative number
		 */
		public int compare(String s1, String s2) {
			String[] split1 = s1.split(BY_SPACE), split2 = s2.split(BY_SPACE);
			int cond = 0;
			for (int i = 0; i < split1.length; i++) {
				cond = split1[i].compareTo(split2[i]);
				if (cond != 0) {
					return cond;
				}
			}
			return cond;
		}
	}
}