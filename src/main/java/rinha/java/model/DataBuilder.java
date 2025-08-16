package rinha.java.model;

public final class DataBuilder {
    private static final ThreadLocal<byte[]> TL = ThreadLocal.withInitial(() -> new byte[96]);

    private DataBuilder() {
    }

    public static byte[] buildBytes(PaymentRequest r) {
        byte[] buf = TL.get();
        int p = 0;

        // correlationId
        System.arraycopy(r.corrBytes, 0, buf, p, r.corrBytes.length);
        p += r.corrBytes.length;

        buf[p++] = (byte) ':';

        // amountCents em ASCII
        p = writeLongAscii(r.amountCents, buf, p);

        buf[p++] = (byte) ':';
        buf[p++] = (byte) (r.isDefault ? '1' : '0');

        byte[] out = new byte[p];
        System.arraycopy(buf, 0, out, 0, p);
        return out;
    }

    private static int writeLongAscii(long v, byte[] b, int pos) {
        if (v == 0) {
            b[pos++] = '0';
            return pos;
        }
        boolean neg = v < 0;
        long x = neg ? -v : v;
        int start = pos, p = pos;
        while (x != 0) {
            long q = x / 10;
            b[p++] = (byte) ('0' + (int) (x - q * 10));
            x = q;
        }
        if (neg) b[p++] = '-';
        for (int i = start, j = p - 1; i < j; i++, j--) {
            byte t = b[i];
            b[i] = b[j];
            b[j] = t;
        }
        return p;
    }
}
