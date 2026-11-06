package com.fromdevtodev.rabbitmq.direct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Executable versions of the claims in the Direct Exchange episode. No broker is required:
 * routing is predicted by {@link Rules#queuesFor}, and the rest is pure parsing and formatting.
 */
class DirectTest {

    @Test
    void errorMessagesReachErrorsPagerAndAudit() {
        List<String> queues = Rules.queuesFor(Rules.ERROR);
        assertEquals(List.of(Rules.ERRORS_PAGER, Rules.AUDIT), queues);
    }

    @Test
    void warningMessagesReachAuditAndMetrics() {
        List<String> queues = Rules.queuesFor(Rules.WARNING);
        assertEquals(List.of(Rules.AUDIT, Rules.METRICS), queues);
    }

    @Test
    void infoMessagesReachOnlyAudit() {
        assertEquals(List.of(Rules.AUDIT), Rules.queuesFor(Rules.INFO));
    }

    @Test
    void debugMessagesMatchNoBinding() {
        assertTrue(Rules.queuesFor(Rules.DEBUG).isEmpty(),
                "debug has no binding; a direct exchange drops it (or returns it if mandatory)");
    }

    @Test
    void eachQueueAppearsAtMostOnceEvenWithSeveralKeys() {
        // audit is bound to info, warning and error; a single message still yields one entry
        List<String> forError = Rules.queuesFor(Rules.ERROR);
        assertEquals(1, forError.stream().filter(q -> q.equals(Rules.AUDIT)).count());
    }

    @Test
    void bindingsOfTheStoryAreExactlyTheOnesDocumented() {
        assertTrue(Rules.BINDINGS.contains(new Rules.Binding(Rules.ERRORS_PAGER, Rules.ERROR)));
        assertTrue(Rules.BINDINGS.contains(new Rules.Binding(Rules.AUDIT, Rules.INFO)));
        assertTrue(Rules.BINDINGS.contains(new Rules.Binding(Rules.AUDIT, Rules.WARNING)));
        assertTrue(Rules.BINDINGS.contains(new Rules.Binding(Rules.AUDIT, Rules.ERROR)));
        assertTrue(Rules.BINDINGS.contains(new Rules.Binding(Rules.METRICS, Rules.WARNING)));
        assertEquals(5, Rules.BINDINGS.size());
    }

    @Test
    void emailQueueIsNotPartOfTheLogsExchangeStory() {
        assertFalse(Rules.QUEUES.contains(Rules.EMAIL),
                "notifications.email is used only for the default-exchange demo");
    }

    @Test
    void messageBodySurvivesAJsonRoundTrip() throws Exception {
        LogEvent original = LogEvent.of(7, Rules.WARNING);
        assertEquals(original, EventJson.fromBytes(EventJson.toBytes(original)));
    }

    @Test
    void statusLineShowsEveryQueueAndMarksMissingOnes() {
        Map<String, Long> ready = new LinkedHashMap<>();
        ready.put(Rules.ERRORS_PAGER, 2L);
        ready.put(Rules.AUDIT, null);
        ready.put(Rules.METRICS, 0L);
        assertEquals(
                "14:03:07 | errors-pager ready 2 | audit does not exist yet | metrics ready 0",
                StatusLine.format(LocalTime.of(14, 3, 7, 999_000_000), ready));
    }

    @Test
    void argsParseNumbersTextAndRequiredOptions() {
        Args args = new Args(new String[] {"publish", "--level", "error", "--count", "5"}, 1);
        assertEquals("error", args.required("level"));
        assertEquals("error", args.text("level", "other"));
        assertEquals("info", args.text("missing", "info"));
        assertEquals(5L, args.number("count", 1));
        assertEquals(1L, args.number("first", 1));
        assertThrows(IllegalArgumentException.class, () -> args.required("missing"));
    }

    @Test
    void argsRejectMalformedInput() {
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "level", "error"}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "--level"}, 1));
    }
}
