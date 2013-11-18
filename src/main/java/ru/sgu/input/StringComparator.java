package ru.sgu.input;

import org.apache.hadoop.io.RawComparator;

/**
 * @author Nikita Konovalov
 */
public class StringComparator implements RawComparator<String> {
    @Override
    public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
        return new Character((char)bytes[0]).compareTo((char)bytes2[0]);
    }

    @Override
    public int compare(String o1, String o2) {
        return o1.compareTo(o2);
    }
}
