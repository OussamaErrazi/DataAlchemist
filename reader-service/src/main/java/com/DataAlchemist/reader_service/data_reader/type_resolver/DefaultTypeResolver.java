package com.DataAlchemist.reader_service.data_reader.type_resolver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DefaultTypeResolver implements TypeResolver{


    private final List<String> dateFormats;

    public DefaultTypeResolver() {
        this.dateFormats = List.of("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy");
    }


    @Override
    public Class<?> resolveType(List<String> values) {
        boolean isInt = true;
        boolean isDouble = true;
        boolean isDate = true;

        for (String value : values) {
            value = value.trim();
            if (value.isEmpty()) continue;

            if (isInt) {
                try { Integer.parseInt(value); }
                catch (NumberFormatException e) { isInt = false; }
            }

            if (isDouble) {
                try { Double.parseDouble(value); }
                catch (NumberFormatException e) { isDouble = false;}
            }

            if(isDate) {
                boolean valid = false;
                for (String dateFormat : dateFormats) {
                    try {
                        new SimpleDateFormat(dateFormat).parse(value);
                        valid = true;
                        break;
                    } catch (ParseException ignored) {}
                }

                if(!valid) isDate = false;
            }

            if(!isInt && !isDouble && !isDate) break;
        }

        if (isInt) return Integer.class;
        if (isDouble) return Double.class;
        if (isDate) return Date.class;
        return String.class;
    }
}
