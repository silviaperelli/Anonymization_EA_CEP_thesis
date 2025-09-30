package evaluation;

import java.time.LocalDateTime;

// Data structure to represent a single sequence in the target dataset
public record Sequence(LocalDateTime startTime, LocalDateTime endTime) {}
