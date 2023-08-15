package com.github.germanosin.postgresql.cdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PgArrayUtil {
  public static List<Long> parseLongArray(String text) {
    if (text.equals("{}")) {
      return Collections.emptyList();
    }
    String[] items = text.substring(1, text.length() - 1).split(",");
    ArrayList<Long> result = new ArrayList<>(items.length);
    for (String item : items) {
      try {
        result.add(Long.parseLong(item, 10));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("not a long array: " + text, e);
      }
    }
    return result;
  }

  public static List<Integer> parseIntArray(String text) {
    return parseLongArray(text).stream().map(Long::intValue).collect(Collectors.toList());
  }

  public static List<Double> parseDoubleArray(String text) {
    if (text.equals("{}")) {
      return Collections.emptyList();
    }
    String[] items = text.substring(1, text.length() - 1).split(",");
    ArrayList<Double> result = new ArrayList<>(items.length);
    for (String item : items) {
      try {
        result.add(Double.parseDouble(item));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("not a double array: " + text, e);
      }
    }
    return result;
  }

  public static List<String> parseStringArray(String text) {
    if (text.equals("{}")) {
      return Collections.emptyList();
    }
    return StringParser.parse(text);
  }



  private static class StringParser {

    private final String source;
    private int pos = 0;

    public StringParser(String source) {
      this.source = source;
    }

    public static List<String> parse(String source) {
      return new StringParser(source).parse();
    }

    public List<String> parse() {
      eat('{');
      List<String> result = new ArrayList<>();
      while (!is('}')) {
        result.add(parseElement());
        if (!is('}')) {
          eat(',');
        }
      }
      eat('}');
      eof();
      return result;
    }

    private void expect(char c) {
      char got = peek();
      if (got != c) {
        throw new IllegalArgumentException(String.format("expect %c at %s, got %c",
            c, formatPosition(), got
        ));
      }
    }

    private String formatPosition(int adjust) {
      int position = this.pos + adjust;
      return String.format("%d ('%s'^'%s')", position, source.substring(0, position),
          source.substring(position));
    }

    private String formatPosition() {
      return formatPosition(0);
    }

    private char peek() {
      if (pos >= source.length()) {
        throw new IllegalArgumentException("expect a char, got eof");
      }
      return source.charAt(pos);
    }

    private String peek(int length) {
      return source.substring(pos, pos + length);
    }

    private char read() {
      char c = peek();
      pos++;
      return c;
    }

    private String read(int length) {
      String result = peek(length);
      pos += length;
      return result;
    }

    private void eat(char c) {
      expect(c);
      pos++;
    }

    private void eat() {
      pos++;
    }

    private void eof() {
      if (pos != source.length()) {
        throw new IllegalArgumentException(String.format("expect eof at %s",
            formatPosition()
        ));
      }
    }

    private boolean is(char c) {
      return peek() == c;
    }

    private String parseElement() {
      if (is('"')) {
        return parseQuotedElement();
      } else {
        return parseUnquotedElement();
      }
    }

    private String parseUnquotedElement() {
      char c = peek();
      StringBuilder sb = new StringBuilder();
      while (c != ',' && c != '{' && c != '}' && c != '"') {
        eat();
        sb.append(c);
        c = peek();
      }
      return sb.toString();
    }

    private String parseQuotedElement() {
      eat('"');
      StringBuilder sb = new StringBuilder();
      char c;
      while ((c = read()) != '"') {
        if (c == '\\') {
          c = read();
          if (c == 'b') {
            sb.append('\b');
          } else if (c == 'f') {
            sb.append('\f');
          } else if (c == 'n') {
            sb.append('\n');
          } else if (c == 'r') {
            sb.append('\r');
          } else if (c == 't') {
            sb.append('\t');
          } else if (c == 'u') {
            String hex = read(4);
            try {
              int hexInt = Integer.parseInt(hex, 16);
              sb.append((char) hexInt);
            } catch (NumberFormatException e) {
              throw new IllegalArgumentException(
                  String.format("expect a 4 chars hex number at %s", formatPosition(-4)));
            }
          } else if (c == '"') {
            sb.append('"');
          } else if (c == '\\') {
            sb.append('\\');
          } else {
            throw new IllegalArgumentException(
                String.format("unknown escaped char: %c at %s",
                    c, formatPosition(-1)));
          }
        } else {
          sb.append(c);
        }
      }
      return sb.toString();
    }
  }
}
