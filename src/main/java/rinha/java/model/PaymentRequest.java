package rinha.java.model;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class PaymentRequest {
    public BigDecimal amount;
    public long requestedAt;
    public boolean isDefault;
    public byte[] requestData;
    private byte[] postData;
    public long amountCents;
    public byte[] corrBytes;

    public PaymentRequest(byte[] requestData) {
        ByteBuffer buf = ByteBuffer.wrap(requestData);
        // carimbo do handler
        long ingressEpochMs = buf.getLong();

        this.requestData = new byte[requestData.length - 8];
        buf.get(this.requestData);

        this.requestedAt = ingressEpochMs;

        // Pegando o correlationId
        byte[] corr = new byte[36];
        int ignored = extractCorrelationIdUuid36(requestData, requestData.length, corr);
        this.corrBytes = corr;
    }

    public PaymentRequest parse(String json) {
        int kAmt = json.indexOf("\"amount\"");
        if (kAmt < 0) throw new IllegalArgumentException("amount missing");
        int cAmt = json.indexOf(':', kAmt);
        int sAmt = nextNonSpace(json, cAmt + 1);
        int eAmt = findNumberEnd(json, sAmt);
        this.amount = new BigDecimal(json.substring(sAmt, eAmt));

        int kReq = json.indexOf("\"requestedAt\"");
        if (kReq < 0) throw new IllegalArgumentException("requestedAt missing");
        int cReq = json.indexOf(':', kReq);
        int q1 = json.indexOf('"', cReq + 1);
        int q2 = json.indexOf('"', q1 + 1);
        if (q1 < 0 || q2 < 0) throw new IllegalArgumentException("requestedAt format");
        this.requestedAt = Instant.parse(json.substring(q1 + 1, q2)).toEpochMilli();

        this.amountCents = amount.movePointRight(2).setScale(0, java.math.RoundingMode.UNNECESSARY).longValueExact();
        return this;
    }

    public PaymentRequest parseToDefault() {
        var paymentRequest = parse(new String(postData, StandardCharsets.UTF_8));
        paymentRequest.isDefault = true;
        return paymentRequest;
    }

    public PaymentRequest parseToFallback() {
        var paymentRequest = parse(new String(postData, StandardCharsets.UTF_8));
        paymentRequest.isDefault = false;
        return paymentRequest;
    }

    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(256));
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.UTC);

    private static final byte[] REQ_AT_PREFIX = ",\"requestedAt\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] QUOTE_BRACE = "\"}".getBytes(StandardCharsets.UTF_8);

    private static final byte[] KEY = "\"correlationId\"".getBytes(StandardCharsets.US_ASCII);
    private static final int[] SKIP = buildSkip(KEY);

    public byte[] getPostData() {
        if (postData != null) return postData;

        byte[] src = requestData;

        int start = 0;
        if (src.length >= 9 && src[0] != '{' && src[8] == '{') {
            start = 8; // ignora o timestamp (ts + JSON)
        }

        int n = src.length - start;
        if (n < 2 || src[start] != '{' || src[start + n - 1] != '}') {
            postData = src;
            return postData;
        }

        //byte[] ts = FORMATTER.format(Instant.now()).getBytes(StandardCharsets.UTF_8);

        long epochMs;
        if (start == 8) {
            epochMs = ByteBuffer.wrap(src, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
        } else {
            epochMs = System.currentTimeMillis(); // fallback
        }

        byte[] ts = FORMATTER.format(Instant.ofEpochMilli(epochMs)).getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = BUFFER.get();
        buf.clear();

        buf.put(src, start, n - 1);
        buf.put(REQ_AT_PREFIX);
        buf.put(ts);
        buf.put(QUOTE_BRACE);

        byte[] out = new byte[buf.position()];
        buf.flip();
        buf.get(out);
        postData = out;
        return postData;
    }

    static int extractCorrelationIdUuid36(byte[] json, int len, byte[] dest) {
        final int end = 0 + len;


        final int k = bmhIndexOf(json, 0, end, KEY, SKIP);
        if (k < 0) return -1;

        int i = k + KEY.length;
        while (i < end && json[i] != (byte) ':') i++;
        if (i >= end) return -1;
        i++; // após ':'

        // 3) pula espaços
        while (i < end) {
            byte c = json[i];
            if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                i++;
                continue;
            }
            break;
        }

        if (i >= end || json[i] != (byte) '"') return -1;
        i++;

        final int startVal = i;
        while (i < end && json[i] != (byte) '"') i++;
        if (i >= end) return -1;

        final int n = i - startVal;
        if (n != 36) return -1;
        if (n > dest.length) return -1;

        final int p = startVal;
        if (json[p + 8] != '-' || json[p + 13] != '-' || json[p + 18] != '-' || json[p + 23] != '-') return -1;

        System.arraycopy(json, startVal, dest, 0, n);
        return n;
    }

    private static int[] buildSkip(byte[] pat) {
        int[] skip = new int[256];
        int last = pat.length - 1;
        for (int i = 0; i < 256; i++) skip[i] = pat.length;
        for (int i = 0; i < last; i++) {
            skip[pat[i] & 0xFF] = last - i;
        }
        return skip;
    }

    private static int bmhIndexOf(byte[] text, int from, int to, byte[] pat, int[] skip) {
        final int n = to - from;
        final int m = pat.length;
        if (m == 0) return from;
        if (n < m) return -1;

        int i = from + m - 1;
        final int last = m - 1;

        while (i < to) {
            int k = i;
            int j = last;
            // compara de trás pra frente
            while (j >= 0 && text[k] == pat[j]) {
                k--;
                j--;
            }
            if (j < 0) {
                // casou
                return k + 1;
            }
            // salto pelo byte "ruim" atual
            i += skip[text[i] & 0xFF];
        }
        return -1;
    }

    private static int nextNonSpace(String s, int i) {
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c != ' ' && c != '\n' && c != '\r' && c != '\t') return i;
            i++;
        }
        return n;
    }

    private static int findNumberEnd(String s, int i) {
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if ((c >= '0' && c <= '9') || c == '.' || c == '-') {
                i++;
            } else {
                break;
            }
        }
        return i;
    }
}
