import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.log_utils import *
from utils.indicator_utils import add_kernel_reg_smoothed_line


light_palette = {}
light_palette["bg_color"] = "#ffffff"
light_palette["plot_bg_color"] = "#ffffff"
light_palette["grid_color"] = "#e6e6e6"
light_palette["text_color"] = "#2e2e2e"
light_palette["dark_candle"] = "#4d98c4"
light_palette["light_candle"] = "#cccccc"
light_palette["volume_color"] = "#f5f5f5"
light_palette["border_color"] = "#2e2e2e"
light_palette["color_1"] = "#5c285b"
light_palette["color_2"] = "#802c62"
light_palette["color_3"] = "#a33262"
light_palette["color_4"] = "#c43d5c"
light_palette["color_5"] = "#de4f51"
light_palette["color_6"] = "#f26841"
light_palette["color_7"] = "#fd862b"
light_palette["color_8"] = "#ffa600"
light_palette["color_9"] = "#3366d6"


def plot_pullback_chart(symbol,
                       df,
                       plots_dir="plots",
                       file_name="channels.png"):
    logi(f"Plotting pullback chart for {symbol}")
    palette = light_palette

    title = file_name.replace(".png", "").replace("-", " ")

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.5, 0.1, 0.2, 0.2], vertical_spacing=0.05,
        subplot_titles=("Price & Trend", "Volume", "RSI", "ADX")
    )

    # Plot candlesticks
    fig.add_trace(go.Candlestick(x=df.index, open=df['open'], high=df['high'], low=df['low'],
                                 close=df['close'], increasing_line_color=palette['light_candle'],
                                 decreasing_line_color=palette['dark_candle'], name='Price'), row=1, col=1)

    # Plot Smoothed close
    df = add_kernel_reg_smoothed_line(df, column_list=['close'], bandwidth=3, var_type='c')
    fig.add_trace(go.Scatter(x=df.index, y=df['close_smoothed'], mode='lines', line=dict(color='blue', width=1), name='close smoothed'), row=1, col=1)

    # Plot EMAs
    fig.add_trace(go.Scatter(x=df.index, y=df['ema_short'], mode='lines',
                             line=dict(color=palette['color_5'], width=1), name='EMA Short'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['ema_long'], mode='lines',
                             line=dict(color=palette['color_7'], width=1), name='EMA Long'), row=1, col=1)
    # Add volume with color coding based on whether close > open (increasing)
    volume_colors = [light_palette["light_candle"] if df['close'].iloc[i] > df['open'].iloc[i] else light_palette["dark_candle"] for i in range(len(df))]

    # Plot volume
    fig.add_trace(go.Bar(x=df.index, y=df['volume'], marker_color=volume_colors, name='Volume'), row=2, col=1)

    # Plot RSI
    fig.add_trace(go.Scatter(x=df.index, y=df['rsi'], mode='lines',
                             line=dict(color=palette['color_4'], width=2), name='RSI'), row=3, col=1)
    # Add overbought/oversold lines for RSI
    fig.add_hline(y=70, line=dict(color=palette['color_3'], dash='dash'), row=3, col=1, name='Overbought (70)')
    fig.add_hline(y=30, line=dict(color=palette['color_3'], dash='dash'), row=3, col=1, name='Oversold (30)')

    # Plot ADX
    fig.add_trace(go.Scatter(x=df.index, y=df['adx'], mode='lines',
                             line=dict(color=palette['color_6'], width=2), name='ADX'), row=4, col=1)
    # Add a reference line at ADX = 25 for trend strength
    fig.add_hline(y=25, line=dict(color=palette['grid_color'], dash='dash'), row=4, col=1, name='ADX Threshold (25)')


    fig.update_layout(title={'text': title, 'x': 0.5},
                      font=dict(family="Verdana", size=12, color=palette["text_color"]),
                      autosize=True, width=1280, height=720,
                      plot_bgcolor=palette["plot_bg_color"],
                      paper_bgcolor=palette["bg_color"])

    fig.update_xaxes(showline=True, linewidth=1, linecolor=palette["grid_color"], gridcolor=palette["grid_color"], rangeslider_visible=False)
    fig.update_yaxes(showline=True, linewidth=1, linecolor=palette["grid_color"], gridcolor=palette["grid_color"])

    # Update y-axis for the volume chart
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    os.makedirs(plots_dir, exist_ok=True)
    fig.write_image(os.path.join(plots_dir, file_name), format="png")

    return fig
