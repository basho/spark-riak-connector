package com.basho.spark.connector.demos.ofac

import java.awt.{Paint, Color}
import java.awt.image.IndexColorModel

import org.jfree.chart.axis.{AxisLocation, NumberAxis, SymbolAxis}
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.PaintScale
import org.jfree.chart.renderer.xy.XYBlockRenderer
import org.jfree.chart.title.PaintScaleLegend
import org.jfree.data.xy.MatrixSeriesCollection
import org.jfree.ui.{RectangleInsets, RectangleEdge, RectangleAnchor}

import scalax.chart.XYChart
import scalax.chart.module.Imports.ChartTheme


// HeatMap implementation based on the JFreeChart's XYBlockRenderer and PaintScale
object HeatMapChart {

  // Color palette for sequential data
  private val colorComp =  Array(
    new Color(255, 245, 235),
    new Color(254, 230, 206),
    new Color(254, 220, 190),
    new Color(253, 210, 170),
    new Color(253, 200, 160),
    new Color(245, 190, 160),
    new Color(245, 180, 152),
    new Color(245, 174, 107),
    new Color(241, 141, 60),
    new Color(241, 105, 19),
    new Color(235, 100, 19),
    new Color(217, 72, 1),
    new Color(210, 60, 1),
    new Color(140, 45, 4)
  )

  def apply(dataset: MatrixSeriesCollection,
            title: String,
            legend: Boolean = true,
            rangeLabels: Array[String],
            domainLabels: Array[String])
           (implicit theme: ChartTheme = ChartTheme.Default): XYChart = {

    // Configure X/Y axises of the heatmap
    val domainAxis = new SymbolAxis("", domainLabels)
    domainAxis.setVerticalTickLabels(true)
    domainAxis.setLowerMargin(0.0)
    domainAxis.setUpperMargin(0.0)
    domainAxis.setTickLabelPaint(Color.black)

    val rangeAxis = new SymbolAxis("", rangeLabels)
    rangeAxis.setLowerMargin(0.0)
    rangeAxis.setUpperMargin(0.0)
    rangeAxis.setTickLabelPaint(Color.black)

    val cm = new IndexColorModel(3,
      colorComp.length,
      colorComp.map(_.getRed.toByte),
      colorComp.map(_.getGreen.toByte),
      colorComp.map(_.getBlue.toByte)
    )

    // Use XYBlockRenderer to represent MatrixSeriesCollection
    val renderer  = new XYBlockRenderer()
    val lower = 1
    val upper = 36
    val ps = new ColorPaintScale(cm, false, lower, upper)
    renderer.setPaintScale(ps)
    renderer.setBlockAnchor(RectangleAnchor.CENTER)
    renderer.setBlockHeight(1.0)
    renderer.setBlockWidth(1.0)

    val plot = new XYPlot(dataset, domainAxis, rangeAxis, renderer)
    plot.setDomainGridlinesVisible(false)
    plot.setRangeGridlinesVisible(false)
    plot.setBackgroundPaint(Color.white)

    // Add color scale axis to the legend
    val scaleAxis = new NumberAxis("Scale")
    scaleAxis.setUpperBound(upper)
    scaleAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits())
    scaleAxis.setAxisLinePaint(Color.lightGray)
    scaleAxis.setTickMarkPaint(Color.lightGray)
    scaleAxis.setRange(lower, upper)

    // Put the legend at the right side of the chart
    val psLegend = new PaintScaleLegend(ps, scaleAxis)
    psLegend.setAxisLocation(AxisLocation.BOTTOM_OR_LEFT)
    psLegend.setStripWidth(20)
    psLegend.setAxisOffset(5.0)
    psLegend.setPosition(RectangleEdge.RIGHT)
    psLegend.setBackgroundPaint(Color.white)
    psLegend.setPadding(new RectangleInsets(10,10,80,40))

    val chart = XYChart(plot, title, legend, theme)
    chart.subtitles += psLegend
    chart
  }
}

// Implementation of JFreeChart's PaintScale interface
private class ColorPaintScale(colorModel: IndexColorModel, log: Boolean, lower: Double, upper: Double) extends PaintScale {
  def getUpperBound: Double = upper
  def getLowerBound: Double = lower

  def getPaint(x: Double): Paint = {
    val z = x + 1
    if (z < this.getLowerBound()) {
      new Color(colorModel.getRGB(this.getLowerBound().toInt))
    } else if (z > this.getUpperBound()) {
      new Color(colorModel.getRGB(this.getUpperBound().toInt))
    } else {
      val min = if (log) Math.log(this.getLowerBound()) else this.getLowerBound()
      val max = if (log) Math.log(this.getUpperBound()) else this.getUpperBound()
      val v = if (log) Math.log(z) else z
      val frac = (v.toDouble - min.toDouble) / (max.toDouble - min.toDouble)
      var index = (frac * colorModel.getMapSize).toInt
      if (index == colorModel.getMapSize) index = index - 1

      new Color(colorModel.getRGB(index))
    }
  }
}
