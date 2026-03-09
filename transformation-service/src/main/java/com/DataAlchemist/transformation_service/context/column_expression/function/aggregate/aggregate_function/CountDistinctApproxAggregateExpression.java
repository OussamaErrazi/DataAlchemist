package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.aggregate_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Approximate distinct value counter using the HyperLogLog algorithm.
 * HyperLogLog estimates the number of distinct values in a dataset using a
 * fixed small amount of memory regardless of input size, with an error margin of approximately 2%.
 **
 **
 **/
public class CountDistinctApproxAggregateExpression extends AggregateExpression {
    private final ColumnExpression input;
    private boolean hasFalse = false; private boolean hasTrue = false;
    private ColumnType type =null;
    private String name = null;

    private static final int B = 10;
    private static final int M = 1 << B;
    private final int[] registers = new int[M];

    public CountDistinctApproxAggregateExpression(ColumnExpression input) {
        this.input = input;
    }


    @Override
    public void addRow(Row row) {
        if(input == null) throw new IllegalArgumentException();
        Cell result = input.evaluate(row);
        if(result.getValue() == null || result.getColumnType() == null) throw new IllegalArgumentException();
        if (type == ColumnType.BOOLEAN){
            hasFalse = Boolean.FALSE.equals(result.getValue());
            hasTrue = Boolean.TRUE.equals(result.getValue());
        } else{
            type = result.getColumnType();
            name = result.getColumnName();

            long h = hash(toBytes(result.getValue()));
            int register = (int) (h >>> (64 - B));
            long remaining = h << B;
            int zeroes = countLeadingZeros(remaining);
            registers[register] = Math.max(registers[register], zeroes);
        }

    }

    public long estimate() {
        double sum = 0;
        for (int reg : registers) {
            sum += Math.pow(2, -reg);
        }
        double alpha = 0.7213 / (1 + 1.079 / M);
        return Math.round(alpha * M * M / sum);
    }

    @Override
    public Cell get() {
        if (type == ColumnType.BOOLEAN) {
            int sum = hasTrue ? 1 : 0;
            return Cell.builder()
                    .value(hasFalse ? ++sum : sum)
                    .columnName("count_distinct_approx("+name+")")
                    .columnType(ColumnType.INTEGER)
                    .build();
        }
        return Cell.builder()
                .value((int) estimate())
                .columnName("count_distinct_approx("+name+")")
                .columnType(ColumnType.INTEGER)
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new CountDistinctApproxAggregateExpression(input);
    }

    private byte[] toBytes(Object o) {
        return switch (type) {
            case ColumnType.INTEGER -> ByteBuffer.allocate(8).putInt((Integer) o).array();
            case ColumnType.DOUBLE -> ByteBuffer.allocate(8).putDouble((Double) o).array();
            case ColumnType.STRING -> o.toString().getBytes(StandardCharsets.UTF_8);
            case ColumnType.DATE -> {
                if(o.toString().matches("[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}")) {
                    yield ByteBuffer.allocate(8).putLong(LocalDate.parse(o.toString(), DateTimeFormatter.ofPattern("dd/MM/yyyy")).toEpochDay()).array();
                } else {
                    yield o.toString().getBytes(StandardCharsets.UTF_8);
                }
            }
            default -> new byte[0];
        };
    }

    private long hash(byte[] bytes) {
        long h = 0xcbf29ce484222325L; // FNV offset basis
        for (byte b : bytes) {
            h ^= b;
            h *= 0x100000001b3L;     // FNV prime
        }
        return h;
    }

    private int countLeadingZeros(long hash) {
        return Long.numberOfLeadingZeros(hash) + 1;
    }
}
