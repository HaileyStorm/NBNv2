using System;
using System.Collections.Generic;
using System.Linq;

namespace Nbn.Tools.Workbench.ViewModels;

public static partial class VizActivityCanvasLayoutBuilder
{
    private static Dictionary<uint, CanvasPoint> BuildRegionPositions(
        IEnumerable<uint> regionIds,
        VizActivityCanvasLayoutMode layoutMode,
        out bool used3DProjection,
        out bool fellBackTo2D)
    {
        used3DProjection = false;
        fellBackTo2D = false;
        if (layoutMode == VizActivityCanvasLayoutMode.Axial3DExperimental
            && TryBuildProjected3DRegionPositions(regionIds, out var projected))
        {
            used3DProjection = true;
            return projected;
        }

        if (layoutMode == VizActivityCanvasLayoutMode.Axial3DExperimental)
        {
            fellBackTo2D = true;
        }

        return BuildRegionPositions2D(regionIds);
    }

    private static Dictionary<uint, CanvasPoint> BuildRegionPositions2D(IEnumerable<uint> regionIds)
    {
        var groupsBySlice = regionIds
            .GroupBy(GetRegionSlice)
            .OrderBy(group => group.Key)
            .ToDictionary(group => group.Key, group => group.OrderBy(region => region).ToList());

        const int minSlice = -3;
        const int maxSlice = 3;
        var sliceSpan = Math.Max(1, maxSlice - minSlice);
        var availableWidth = CanvasWidth - (RegionNodePositionPadding * 2.0);
        var availableHeight = CanvasHeight - (RegionNodePositionPadding * 2.0);
        var compressedHalfWidth = (availableWidth * 0.54) / 2.0;
        var maxLaneHalfHeight = Math.Max(70.0, availableHeight * 0.74);
        var positions = new Dictionary<uint, CanvasPoint>();

        foreach (var (slice, regions) in groupsBySlice)
        {
            var normalizedSlice = ((double)(slice - minSlice) / sliceSpan) * 2.0 - 1.0;
            var axisX = CenterX + (normalizedSlice * compressedHalfWidth);
            var depthRatio = 1.0 - (Math.Abs(slice) / (double)maxSlice);
            var sliceWave = slice switch
            {
                -3 or 0 or 3 => 0.0,
                _ => (((slice - minSlice) & 1) == 0 ? 1.0 : -1.0) * (48.0 + (34.0 * depthRatio))
            };
            var rightOverlayBias = normalizedSlice > 0.0 ? normalizedSlice * 34.0 : 0.0;
            var laneCenterY = CenterY + sliceWave + rightOverlayBias;
            var count = regions.Count;
            var laneHalfHeight = count <= 1
                ? 0.0
                : Math.Clamp(
                    (30.0 + ((count - 1) * 18.0)) * (0.9 + (0.36 * depthRatio)),
                    56.0,
                    maxLaneHalfHeight);
            var laneHalfWidth = count <= 1 ? 0.0 : 3.5 + (2.0 * depthRatio);

            for (var index = 0; index < count; index++)
            {
                var laneOffset = count <= 1
                    ? 0.0
                    : ((double)index / (count - 1) * 2.0) - 1.0;
                var x = axisX + (laneOffset * laneHalfWidth);
                var y = laneCenterY + (laneOffset * laneHalfHeight);
                positions[regions[index]] = new CanvasPoint(
                    Clamp(x, RegionNodePositionPadding, CanvasWidth - RegionNodePositionPadding),
                    Clamp(y, RegionNodePositionPadding, CanvasHeight - RegionNodePositionPadding));
            }
        }

        return positions;
    }

    private static bool TryBuildProjected3DRegionPositions(
        IEnumerable<uint> regionIds,
        out Dictionary<uint, CanvasPoint> positions)
    {
        var uniqueRegions = regionIds
            .Distinct()
            .OrderBy(GetRegionSlice)
            .ThenBy(regionId => regionId)
            .ToList();
        positions = new Dictionary<uint, CanvasPoint>(uniqueRegions.Count);
        if (uniqueRegions.Count == 0 || uniqueRegions.Count > Nbn.Shared.NbnConstants.RegionCount)
        {
            return false;
        }

        const int minSlice = -3;
        const int maxSlice = 3;
        const double depthX = 62.0;
        const double depthY = 23.0;
        const double inSliceVertical = 72.0;
        const double inSliceHorizontal = 12.0;

        var groupsBySlice = uniqueRegions
            .GroupBy(GetRegionSlice)
            .OrderBy(group => group.Key)
            .ToDictionary(group => group.Key, group => group.ToList());
        foreach (var (slice, regions) in groupsBySlice)
        {
            var z = slice;
            var depthRatio = 1.0 - (Math.Abs(z) / (double)maxSlice);
            var laneCount = regions.Count;
            for (var index = 0; index < laneCount; index++)
            {
                var lane = laneCount <= 1
                    ? 0.0
                    : ((double)index / (laneCount - 1) * 2.0) - 1.0;
                var localX = lane * inSliceHorizontal * (1.0 + (0.3 * depthRatio));
                var localY = lane * inSliceVertical * (0.7 + (0.45 * depthRatio));
                var x = CenterX + localX + (z * depthX);
                var rightOverlayBias = z > 0 ? (z / (double)maxSlice) * 20.0 : 0.0;
                var y = CenterY + localY - (z * depthY) + rightOverlayBias;
                x = Clamp(x, RegionNodePositionPadding, CanvasWidth - RegionNodePositionPadding);
                y = Clamp(y, RegionNodePositionPadding, CanvasHeight - RegionNodePositionPadding);
                positions[regions[index]] = new CanvasPoint(x, y);
            }
        }

        var centersX = positions.Values.Select(point => point.X).ToList();
        var centersY = positions.Values.Select(point => point.Y).ToList();
        var xSpread = centersX.Max() - centersX.Min();
        var ySpread = centersY.Max() - centersY.Min();
        if (!double.IsFinite(xSpread) || !double.IsFinite(ySpread))
        {
            positions.Clear();
            return false;
        }

        var minimumExpectedXSpread = (maxSlice - minSlice) * 22.0;
        if (xSpread < minimumExpectedXSpread || ySpread < 48.0)
        {
            positions.Clear();
            return false;
        }

        return true;
    }

    private static IReadOnlyList<CanvasPoint> BuildConcentricPositions(
        int count,
        double minRadius,
        double maxRadius,
        double yScale,
        double minCenterSpacing,
        double ringGapPadding = 12.0,
        double minRingGap = 24.0,
        bool clampToCanvas = true)
    {
        if (count <= 0)
        {
            return Array.Empty<CanvasPoint>();
        }

        if (count == 1)
        {
            return new[] { new CanvasPoint(CenterX, CenterY) };
        }

        var safeYScale = Math.Clamp(yScale, 0.45, 1.15);
        var safeSpacing = Math.Max(8.0, minCenterSpacing);
        var safeMinRadius = Math.Max(0.0, minRadius);
        var safeMaxRadius = Math.Max(safeMinRadius, maxRadius);
        var safeRingGapPadding = Math.Max(0.0, ringGapPadding);
        var safeMinRingGap = Math.Max(8.0, minRingGap);
        var ringGap = Math.Max(safeMinRingGap, safeSpacing + safeRingGapPadding);
        var positions = new List<CanvasPoint>(count);

        var remaining = count;
        var ringIndex = 0;
        while (remaining > 0)
        {
            var radius = safeMinRadius + (ringIndex * ringGap);
            if (radius > safeMaxRadius)
            {
                radius = safeMaxRadius;
            }

            var effectiveCircumference = 2.0 * Math.PI * Math.Max(1.0, radius * safeYScale);
            var naturalCapacity = Math.Max(6, (int)Math.Floor(effectiveCircumference / safeSpacing));
            var ringCount = Math.Min(naturalCapacity, remaining);
            var nextRadius = Math.Min(safeMaxRadius, radius + ringGap);
            var nextRingGap = nextRadius - radius;
            var canPlaceSafeNextRing = nextRingGap >= (safeSpacing - 0.5);
            if (remaining > ringCount && !canPlaceSafeNextRing)
            {
                ringCount = remaining;
                naturalCapacity = Math.Max(naturalCapacity, ringCount);
            }

            var angleStep = (2.0 * Math.PI) / naturalCapacity;
            var ringPhase = (ringIndex * Math.PI) / Math.Max(3, naturalCapacity);
            var applyOverlayNudge = naturalCapacity >= 12;
            for (var index = 0; index < ringCount; index++)
            {
                var slot = ringCount == naturalCapacity
                    ? index
                    : (int)Math.Floor(((index + 0.5) * naturalCapacity) / ringCount);
                var angle = ringPhase + (slot * angleStep);
                if (applyOverlayNudge)
                {
                    angle = NudgeAngleAwayFromOverlay(angle);
                }

                var x = CenterX + (Math.Cos(angle) * radius);
                var y = CenterY + (Math.Sin(angle) * radius * safeYScale);
                positions.Add(clampToCanvas
                    ? new CanvasPoint(
                        Clamp(x, CanvasPadding, CanvasWidth - CanvasPadding),
                        Clamp(y, CanvasPadding, CanvasHeight - CanvasPadding))
                    : new CanvasPoint(x, y));
            }

            remaining -= ringCount;
            ringIndex++;
            if (radius >= safeMaxRadius)
            {
                break;
            }
        }

        if (positions.Count >= count)
        {
            return positions;
        }

        var fallbackRadius = safeMaxRadius;
        var fallbackRemaining = count - positions.Count;
        var fallbackStep = (2.0 * Math.PI) / fallbackRemaining;
        var applyFallbackOverlayNudge = fallbackRemaining >= 12;
        for (var index = 0; index < fallbackRemaining; index++)
        {
            var angle = (index * fallbackStep) + (Math.PI / 7.0);
            if (applyFallbackOverlayNudge)
            {
                angle = NudgeAngleAwayFromOverlay(angle);
            }

            var x = CenterX + (Math.Cos(angle) * fallbackRadius);
            var y = CenterY + (Math.Sin(angle) * fallbackRadius * safeYScale);
            positions.Add(clampToCanvas
                ? new CanvasPoint(
                    Clamp(x, CanvasPadding, CanvasWidth - CanvasPadding),
                    Clamp(y, CanvasPadding, CanvasHeight - CanvasPadding))
                : new CanvasPoint(x, y));
        }

        return positions;
    }

    private static Dictionary<uint, CanvasPoint> BuildFocusedGatewayPositions(
        IReadOnlyList<uint> gatewayRegionIds,
        uint focusRegionId,
        double minRadius,
        double maxRadius,
        double yScale,
        bool clampToCanvas)
    {
        var positions = new Dictionary<uint, CanvasPoint>();
        if (gatewayRegionIds.Count == 0)
        {
            return positions;
        }

        var anchorRegions = new HashSet<uint>(gatewayRegionIds)
        {
            focusRegionId
        };
        var referencePositions = BuildRegionPositions2D(anchorRegions);
        var hasFocusReference = referencePositions.TryGetValue(focusRegionId, out var focusReference);
        if (!hasFocusReference)
        {
            focusReference = new CanvasPoint(CenterX, CenterY);
        }

        var safeYScale = Math.Clamp(yScale, 0.45, 1.15);
        var baseRadius = Math.Clamp(minRadius + (gatewayRegionIds.Count * 3.0), minRadius, maxRadius);
        var minCenterSpacing = (MaxGatewayNodeRadius * 2.0) + 2.0;
        var radiusStep = Math.Max(minCenterSpacing + 2.0, 22.0);
        var countBasedJitterStep = Math.PI / Math.Max(24.0, gatewayRegionIds.Count * 3.0);
        var spacingBasedJitterStep = (minCenterSpacing * 1.05) / Math.Max(baseRadius * safeYScale, 1.0);
        var angleJitterStep = Math.Max(countBasedJitterStep, spacingBasedJitterStep);
        var angleOffsetOrder = BuildSignedOffsetOrder(maxMagnitude: 12);
        var maxRadiusBand = Math.Max(12, gatewayRegionIds.Count);

        var fallbackStep = (2.0 * Math.PI) / Math.Max(1, gatewayRegionIds.Count);
        var referenceAngles = new Dictionary<uint, double>();
        for (var index = 0; index < gatewayRegionIds.Count; index++)
        {
            var regionId = gatewayRegionIds[index];
            var hasReference = referencePositions.TryGetValue(regionId, out var referencePoint);
            var angle = hasReference
                ? Math.Atan2((referencePoint.Y - focusReference.Y) / safeYScale, referencePoint.X - focusReference.X)
                : (index * fallbackStep);
            referenceAngles[regionId] = angle;
        }

        var orderedRegionIds = gatewayRegionIds
            .Distinct()
            .OrderBy(regionId => NormalizeAngle(referenceAngles[regionId]))
            .ThenBy(regionId => regionId)
            .ToList();

        foreach (var regionId in orderedRegionIds)
        {
            var baseAngle = referenceAngles[regionId];
            var bestCandidate = BuildFocusedGatewayPoint(baseAngle, baseRadius, safeYScale, clampToCanvas);
            var bestCandidateDistance = MinDistanceToPlaced(bestCandidate, positions);
            var placed = false;

            for (var radiusBand = 0; radiusBand <= maxRadiusBand && !placed; radiusBand++)
            {
                var rawRadius = baseRadius + (radiusBand * radiusStep);
                var candidateRadius = clampToCanvas
                    ? Math.Clamp(rawRadius, minRadius, maxRadius)
                    : Math.Max(minRadius, rawRadius);
                foreach (var angleOffset in angleOffsetOrder)
                {
                    var candidateAngle = baseAngle + (angleOffset * angleJitterStep);
                    var candidate = BuildFocusedGatewayPoint(candidateAngle, candidateRadius, safeYScale, clampToCanvas);
                    var candidateDistance = MinDistanceToPlaced(candidate, positions);
                    if (candidateDistance > bestCandidateDistance)
                    {
                        bestCandidateDistance = candidateDistance;
                        bestCandidate = candidate;
                    }

                    if (candidateDistance >= minCenterSpacing - 0.05)
                    {
                        bestCandidate = candidate;
                        placed = true;
                        break;
                    }
                }
            }

            positions[regionId] = bestCandidate;
        }

        return positions;
    }

    private static CanvasPoint BuildFocusedGatewayPoint(
        double angle,
        double radius,
        double yScale,
        bool clampToCanvas)
    {
        angle = NudgeAngleAwayFromOverlay(angle);
        var x = CenterX + (Math.Cos(angle) * radius);
        var y = CenterY + (Math.Sin(angle) * radius * yScale);
        if (!clampToCanvas)
        {
            return new CanvasPoint(x, y);
        }

        return new CanvasPoint(
            Clamp(x, CanvasPadding, CanvasWidth - CanvasPadding),
            Clamp(y, CanvasPadding, CanvasHeight - CanvasPadding));
    }

    private static double MinDistanceToPlaced(CanvasPoint candidate, IReadOnlyDictionary<uint, CanvasPoint> placed)
    {
        if (placed.Count == 0)
        {
            return double.PositiveInfinity;
        }

        var minDistanceSquared = double.PositiveInfinity;
        foreach (var point in placed.Values)
        {
            var dx = candidate.X - point.X;
            var dy = candidate.Y - point.Y;
            var distanceSquared = (dx * dx) + (dy * dy);
            if (distanceSquared < minDistanceSquared)
            {
                minDistanceSquared = distanceSquared;
            }
        }

        return double.IsPositiveInfinity(minDistanceSquared) ? double.PositiveInfinity : Math.Sqrt(minDistanceSquared);
    }

    private static double NormalizeAngle(double angle)
    {
        var normalized = angle % (2.0 * Math.PI);
        return normalized < 0 ? normalized + (2.0 * Math.PI) : normalized;
    }

    private static double NormalizeSignedAngle(double angle)
    {
        var normalized = angle % (2.0 * Math.PI);
        if (normalized <= -Math.PI)
        {
            normalized += 2.0 * Math.PI;
        }
        else if (normalized > Math.PI)
        {
            normalized -= 2.0 * Math.PI;
        }

        return normalized;
    }

    private static double NudgeAngleAwayFromOverlay(double angle)
    {
        var signedAngle = NormalizeSignedAngle(angle);
        var delta = NormalizeSignedAngle(signedAngle - OverlayAvoidUpperRightCenterRadians);
        var safeTransition = Math.Max(0.01, OverlayAvoidUpperRightTransitionRadians);
        var safeSpread = Math.Max(safeTransition, OverlayAvoidUpperRightSpreadRadians);
        var direction = Math.Tanh(delta / safeTransition);
        if (Math.Abs(direction) < 1e-6)
        {
            return signedAngle;
        }

        var falloff = Math.Exp(-((delta * delta) / (2.0 * safeSpread * safeSpread)));
        var shift = OverlayAvoidUpperRightMaxShiftRadians * direction * falloff;
        return NormalizeSignedAngle(signedAngle + shift);
    }

    private static int[] BuildSignedOffsetOrder(int maxMagnitude)
    {
        var boundedMagnitude = Math.Max(0, maxMagnitude);
        var offsets = new List<int>(1 + (boundedMagnitude * 2))
        {
            0
        };
        for (var magnitude = 1; magnitude <= boundedMagnitude; magnitude++)
        {
            offsets.Add(magnitude);
            offsets.Add(-magnitude);
        }

        return offsets.ToArray();
    }

    private static CanvasEdgeCurve BuildEdgeCurve(
        CanvasPoint source,
        CanvasPoint target,
        bool isSelfLoop,
        int curveDirection,
        double sourceRadius,
        double targetRadius)
    {
        sourceRadius = Math.Max(0.0, sourceRadius);
        targetRadius = Math.Max(0.0, targetRadius);
        var normalizedDirection = curveDirection < 0 ? -1 : 1;
        var cacheKey = new CanvasEdgeCurveKey(
            QuantizeCurveCoord(source.X),
            QuantizeCurveCoord(source.Y),
            QuantizeCurveCoord(target.X),
            QuantizeCurveCoord(target.Y),
            isSelfLoop,
            normalizedDirection,
            QuantizeCurveCoord(sourceRadius),
            QuantizeCurveCoord(targetRadius));
        lock (EdgeCurveCacheGate)
        {
            if (EdgeCurveCache.TryGetValue(cacheKey, out var cached))
            {
                return cached;
            }
        }

        CanvasEdgeCurve curve;
        if (isSelfLoop)
        {
            var radialOffset = Math.Max(10.0, sourceRadius * 1.02);
            var tangentialOffset = Math.Max(7.0, sourceRadius * 0.38);
            var apexOffset = Math.Max(16.0, sourceRadius * 1.58);
            var start = new CanvasPoint(
                source.X + radialOffset,
                source.Y - tangentialOffset);
            var control = new CanvasPoint(
                source.X + apexOffset,
                source.Y - apexOffset);
            var end = new CanvasPoint(
                source.X + (radialOffset * 0.46),
                source.Y - (radialOffset * 1.04));
            var pathData = FormattableString.Invariant($"M {start.X:0.###} {start.Y:0.###} Q {control.X:0.###} {control.Y:0.###} {end.X:0.###} {end.Y:0.###}");
            curve = new CanvasEdgeCurve(pathData, start, control, end);
        }
        else
        {
            var deltaX = target.X - source.X;
            var deltaY = target.Y - source.Y;
            var length = Math.Sqrt((deltaX * deltaX) + (deltaY * deltaY));
            if (length < 1e-4)
            {
                length = 1e-4;
            }

            var unitX = deltaX / length;
            var unitY = deltaY / length;
            var startOffset = sourceRadius + EdgeNodeClearance;
            var endOffset = targetRadius + EdgeNodeClearance;
            var startX = source.X + (unitX * startOffset);
            var startY = source.Y + (unitY * startOffset);
            var endX = target.X - (unitX * endOffset);
            var endY = target.Y - (unitY * endOffset);
            var adjustedDeltaX = endX - startX;
            var adjustedDeltaY = endY - startY;
            var adjustedLength = Math.Sqrt((adjustedDeltaX * adjustedDeltaX) + (adjustedDeltaY * adjustedDeltaY));
            if (adjustedLength < 4.0)
            {
                startX = source.X + (unitX * (sourceRadius * 0.5));
                startY = source.Y + (unitY * (sourceRadius * 0.5));
                endX = target.X - (unitX * (targetRadius * 0.5));
                endY = target.Y - (unitY * (targetRadius * 0.5));
                adjustedDeltaX = endX - startX;
                adjustedDeltaY = endY - startY;
                adjustedLength = Math.Sqrt((adjustedDeltaX * adjustedDeltaX) + (adjustedDeltaY * adjustedDeltaY));
            }

            var safeLength = Math.Max(adjustedLength, 1e-4);
            var midX = (startX + endX) / 2.0;
            var midY = (startY + endY) / 2.0;
            var normalX = -adjustedDeltaY / safeLength;
            var normalY = adjustedDeltaX / safeLength;
            var curvature = Math.Min(48.0, 16.0 + (length * 0.12)) * normalizedDirection;
            var start = new CanvasPoint(
                startX,
                startY);
            var control = new CanvasPoint(
                midX + (normalX * curvature),
                midY + (normalY * curvature));
            var end = new CanvasPoint(
                endX,
                endY);
            var pathData = FormattableString.Invariant($"M {start.X:0.###} {start.Y:0.###} Q {control.X:0.###} {control.Y:0.###} {end.X:0.###} {end.Y:0.###}");
            curve = new CanvasEdgeCurve(pathData, start, control, end);
        }

        lock (EdgeCurveCacheGate)
        {
            if (!EdgeCurveCache.ContainsKey(cacheKey))
            {
                if (EdgeCurveCache.Count >= EdgeCurveCacheMaxEntries && EdgeCurveCacheOrder.Count > 0)
                {
                    var evicted = EdgeCurveCacheOrder.Dequeue();
                    EdgeCurveCache.Remove(evicted);
                }

                EdgeCurveCache[cacheKey] = curve;
                EdgeCurveCacheOrder.Enqueue(cacheKey);
                return curve;
            }

            return EdgeCurveCache[cacheKey];
        }
    }

    private static int QuantizeCurveCoord(double value)
        => (int)Math.Round(value * 1000.0, MidpointRounding.AwayFromZero);

    private readonly record struct CanvasPoint(double X, double Y);

    private readonly record struct CanvasEdgeCurve(string PathData, CanvasPoint Start, CanvasPoint Control, CanvasPoint End);

    private readonly record struct CanvasEdgeCurveKey(
        int SourceX,
        int SourceY,
        int TargetX,
        int TargetY,
        bool IsSelfLoop,
        int CurveDirection,
        int SourceRadius,
        int TargetRadius);
}
