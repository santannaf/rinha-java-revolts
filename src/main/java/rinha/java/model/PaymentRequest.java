package rinha.java.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
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
    public String correlationId;
    public long amountCents;
    public byte[] corrBytes;

    public PaymentRequest(byte[] requestData) {
        this.requestData = requestData;
        // Pegando o correlationId
        byte[] corr = new byte[36];
        int n = extractCorrelationIdUuid36(requestData, requestData.length, corr);
        this.corrBytes = corr;
    }

    public PaymentRequest parse(String json) {
        int startAmount = json.lastIndexOf(':') + 1;
        this.amount = new BigDecimal(json.substring(startAmount, json.length() - 1).trim());
        this.requestedAt = Instant.parse(json.substring(16, json.indexOf('Z') + 1)).toEpochMilli();
        this.amountCents = amount.movePointRight(2).setScale(0, RoundingMode.UNNECESSARY).longValueExact();
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

    private static final byte[] REQUESTED_AT_PREFIX = "{\"requestedAt\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] REQUESTED_AT_SUFFIX = "\",".getBytes(StandardCharsets.UTF_8);

    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(256));
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.UTC);

    public byte[] getPostData() {
        if (postData == null) {
            ByteBuffer buffer = BUFFER.get().clear();
            buffer.put(REQUESTED_AT_PREFIX);

            Instant now = Instant.ofEpochMilli(System.currentTimeMillis());
            buffer.put(FORMATTER.format(now).getBytes(StandardCharsets.UTF_8));

            buffer.put(REQUESTED_AT_SUFFIX);

            buffer.put(requestData, 1, requestData.length - 1);

            byte[] result = new byte[buffer.position()];
            buffer.flip();
            buffer.get(result);
            postData = result;
        }

        return postData;
    }

    private static final byte[] KEY = "\"correlationId\"".getBytes(StandardCharsets.US_ASCII);
    private static final int[] SKIP = buildSkip(KEY);

    static int extractCorrelationIdUuid36(byte[] json, int len, byte[] dest) {
        final int end = 0 + len;

        // 1) encontrar a chave usando Boyer–Moore–Horspool (saltos rápidos)
        final int k = bmhIndexOf(json, 0, end, KEY, SKIP);
        if (k < 0) return -1;

        // 2) avança até ':' após a chave
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

        // 4) espera aspas de abertura
        if (i >= end || json[i] != (byte) '"') return -1;
        i++; // início do valor

        // 5) encontra aspas de fechamento
        final int startVal = i;
        while (i < end && json[i] != (byte) '"') i++;
        if (i >= end) return -1;

        final int n = i - startVal;          // tamanho do valor
        if (n != 36) return -1;              // UUID deve ter 36
        if (n > dest.length) return -1;

        // valida hífens (posições 8, 13, 18, 23)
        final int p = startVal;
        if (json[p + 8] != '-' || json[p + 13] != '-' || json[p + 18] != '-' || json[p + 23] != '-') return -1;

        // 6) copia para o destino
        System.arraycopy(json, startVal, dest, 0, n);
        return n;
    }

    private static int[] buildSkip(byte[] pat) {
        int[] skip = new int[256];
        int last = pat.length - 1;
        // default: saltar o tamanho inteiro
        for (int i = 0; i < 256; i++) skip[i] = pat.length;
        // para cada byte do padrão (menos o último), define salto
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

        int i = from + m - 1; // índice no texto apontando pro char alinhado ao final do padrão
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
}
