import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

DATA_DIR = "/home/bdn/bitcoin-analysis/data"
OUTPUT_DIR = "/home/bdn/bitcoin-analysis/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

plt.rcParams.update(
    {
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.grid": True,
        "grid.alpha": 0.3,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "font.family": "sans-serif",
        "font.size": 11,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.labelsize": 12,
        "legend.fontsize": 10,
        "figure.dpi": 150,
    }
)

COLORS = {
    "BTC": "#F7931A",
    "AAPL": "#555555",
    "GOOGL": "#4285F4",
    "MSFT": "#00A4EF",
    "SPY": "#6B21A8",
    "EUR": "#003399",
    "GBP": "#C8102E",
    "USD": "#22C55E",
    "positive": "#16A34A",
    "negative": "#DC2626",
    "neutral": "#6B7280",
}


def load_prices():
    path = os.path.join(DATA_DIR, "intermediate/int_market__unified_prices.parquet")
    df = pd.read_parquet(path)
    df["data_date"] = pd.to_datetime(df["data_date"]).dt.date
    return df


def generate_q1_chart():
    print("Generating Q1: Performance vs Bitcoin chart...")

    prices = load_prices()

    end_date = prices.groupby("asset_id")["data_date"].max().min()

    windows = {
        "7D": end_date - timedelta(days=7),
        "1M": end_date - timedelta(days=30),
        "3M": end_date - timedelta(days=91),
        "6M": end_date - timedelta(days=182),
        "YTD": datetime(end_date.year, 1, 1).date(),
        "1Y": end_date - timedelta(days=365),
    }

    results = []
    for period_name, start_date in windows.items():
        for asset in prices["asset_id"].unique():
            asset_prices = prices[prices["asset_id"] == asset].sort_values("data_date")

            start_prices = asset_prices[asset_prices["data_date"] >= start_date]
            if len(start_prices) == 0:
                continue
            start_price = start_prices.iloc[0]["close_price"]

            end_prices = asset_prices[asset_prices["data_date"] <= end_date]
            if len(end_prices) == 0:
                continue
            end_price = end_prices.iloc[-1]["close_price"]

            ret = (end_price / start_price - 1) * 100
            results.append({"period": period_name, "asset": asset, "return_pct": ret})

    df = pd.DataFrame(results)

    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    axes = axes.flatten()

    period_order = ["1Y", "YTD", "6M", "3M", "1M", "7D"]

    for idx, period in enumerate(period_order):
        ax = axes[idx]
        period_df = df[df["period"] == period].sort_values("return_pct", ascending=True)

        btc_values = period_df[period_df["asset"] == "BTC"]["return_pct"].values
        btc_return = btc_values[0] if len(btc_values) > 0 else 0

        colors = [COLORS.get(asset, "#888888") for asset in period_df["asset"]]

        bars = ax.barh(
            period_df["asset"], period_df["return_pct"], color=colors, alpha=0.85
        )

        ax.axvline(
            btc_return,
            color=COLORS["BTC"],
            linestyle="--",
            linewidth=2,
            alpha=0.8,
            label="BTC",
        )

        for bar, val in zip(bars, period_df["return_pct"]):
            x_pos = val + (0.5 if val >= 0 else -0.5)
            ha = "left" if val >= 0 else "right"
            ax.text(
                x_pos,
                bar.get_y() + bar.get_height() / 2,
                f"{val:+.1f}%",
                va="center",
                ha=ha,
                fontsize=9,
                fontweight="bold",
            )

        ax.set_title(f"{period}", fontsize=13, fontweight="bold", pad=10)
        ax.set_xlabel("Return (%)")
        ax.axvline(0, color="black", linewidth=0.5)

        max_abs = max(
            abs(period_df["return_pct"].max()), abs(period_df["return_pct"].min())
        )
        ax.set_xlim(-max_abs * 1.3, max_abs * 1.3)

    fig.suptitle(
        "Asset Performance by Time Period", fontsize=16, fontweight="bold", y=1.02
    )
    plt.tight_layout()

    output_path = os.path.join(OUTPUT_DIR, "performance_vs_btc.png")
    plt.savefig(output_path, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")

    return df


def generate_q2_chart():
    print("Generating Q2: Investment Value chart...")

    prices = load_prices()
    btc_prices = prices[prices["asset_id"] == "BTC"].sort_values("data_date")

    start_date = btc_prices["data_date"].min()
    start_price = btc_prices.iloc[0]["close_price"]

    initial_investment = 1000
    btc_amount = initial_investment / start_price
    btc_prices = btc_prices.copy()
    btc_prices["investment_value"] = btc_prices["close_price"] * btc_amount

    final_value = btc_prices.iloc[-1]["investment_value"]
    max_value = btc_prices["investment_value"].max()
    min_value = btc_prices["investment_value"].min()

    fig, ax = plt.subplots(figsize=(12, 7))

    dates = pd.to_datetime(btc_prices["data_date"])
    values = btc_prices["investment_value"]

    ax.fill_between(
        dates,
        initial_investment,
        values,
        where=(values >= initial_investment),
        color=COLORS["positive"],
        alpha=0.3,
        label="Gain",
    )
    ax.fill_between(
        dates,
        initial_investment,
        values,
        where=(values < initial_investment),
        color=COLORS["negative"],
        alpha=0.3,
        label="Loss",
    )

    ax.plot(dates, values, color=COLORS["BTC"], linewidth=2.5, label="BTC Investment")
    ax.axhline(
        initial_investment,
        color=COLORS["USD"],
        linestyle="--",
        linewidth=2,
        alpha=0.8,
        label=f"USD (Cash) - ${initial_investment:,.0f}",
    )

    ax.annotate(
        f"${final_value:,.2f}",
        xy=(dates.iloc[-1], final_value),
        xytext=(10, 0),
        textcoords="offset points",
        fontsize=12,
        fontweight="bold",
        color=COLORS["BTC"],
    )

    max_idx = btc_prices["investment_value"].idxmax()
    min_idx = btc_prices["investment_value"].idxmin()

    ax.scatter(
        pd.to_datetime(btc_prices.loc[max_idx, "data_date"]),
        max_value,
        color=COLORS["positive"],
        s=100,
        zorder=5,
        marker="^",
    )
    ax.annotate(
        f"Peak: ${max_value:,.0f}",
        xy=(pd.to_datetime(btc_prices.loc[max_idx, "data_date"]), max_value),
        xytext=(0, 15),
        textcoords="offset points",
        fontsize=10,
        ha="center",
        color=COLORS["positive"],
    )

    ax.scatter(
        pd.to_datetime(btc_prices.loc[min_idx, "data_date"]),
        min_value,
        color=COLORS["negative"],
        s=100,
        zorder=5,
        marker="v",
    )
    ax.annotate(
        f"Trough: ${min_value:,.0f}",
        xy=(pd.to_datetime(btc_prices.loc[min_idx, "data_date"]), min_value),
        xytext=(0, -20),
        textcoords="offset points",
        fontsize=10,
        ha="center",
        color=COLORS["negative"],
    )

    ax.set_title(
        "Value of $1,000 Investment: Bitcoin vs USD (1 Year)",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value (USD)")
    ax.legend(loc="upper right")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.xticks(rotation=45)

    return_pct = (final_value - initial_investment) / initial_investment * 100
    textstr = f"Final Value: ${final_value:,.2f}\nReturn: {return_pct:+.2f}%"
    props = dict(boxstyle="round", facecolor="white", alpha=0.9, edgecolor="gray")
    ax.text(
        0.02,
        0.98,
        textstr,
        transform=ax.transAxes,
        fontsize=11,
        verticalalignment="top",
        bbox=props,
    )

    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "investment_value_1y.png")
    plt.savefig(output_path, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")

    return final_value, return_pct


def generate_q3_chart():
    print("Generating Q3: DCA vs Lump Sum chart...")

    prices = load_prices()
    btc_prices = prices[prices["asset_id"] == "BTC"].sort_values("data_date").copy()
    btc_prices["data_date"] = pd.to_datetime(btc_prices["data_date"])
    btc_prices = btc_prices.set_index("data_date")

    start_date = btc_prices.index.min()
    end_date = btc_prices.index.max()
    end_price = btc_prices.iloc[-1]["close_price"]

    total_investment = 1200
    start_price = btc_prices.iloc[0]["close_price"]
    lump_btc = total_investment / start_price
    btc_prices["lump_sum_value"] = btc_prices["close_price"] * lump_btc

    dca_btc_holdings = pd.Series(0.0, index=btc_prices.index)
    dca_cost_basis = pd.Series(0.0, index=btc_prices.index)

    accumulated_btc = 0
    accumulated_cost = 0

    for month_offset in range(12):
        target_date = start_date + relativedelta(months=month_offset)

        valid_dates = btc_prices.index[btc_prices.index >= target_date]
        if len(valid_dates) > 0:
            buy_date = valid_dates[0]
            buy_price = btc_prices.loc[buy_date, "close_price"]
            btc_bought = 100 / buy_price
            accumulated_btc += btc_bought
            accumulated_cost += 100

            dca_btc_holdings.loc[buy_date:] = accumulated_btc
            dca_cost_basis.loc[buy_date:] = accumulated_cost

    btc_prices["dca_value"] = dca_btc_holdings * btc_prices["close_price"]
    btc_prices["dca_cost"] = dca_cost_basis

    final_lump = btc_prices["lump_sum_value"].iloc[-1]
    final_dca = btc_prices["dca_value"].iloc[-1]

    lump_return = (final_lump - 1200) / 1200 * 100
    dca_return = (final_dca - 1200) / 1200 * 100

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(
        btc_prices.index,
        btc_prices["lump_sum_value"],
        color="#8B5CF6",
        linewidth=2.5,
        label="Lump Sum ($1,200)",
    )
    ax.plot(
        btc_prices.index,
        btc_prices["dca_value"],
        color="#0EA5E9",
        linewidth=2.5,
        label="DCA Value",
    )
    ax.plot(
        btc_prices.index,
        btc_prices["dca_cost"],
        color="#6B7280",
        linewidth=1.5,
        linestyle="--",
        alpha=0.7,
        label="DCA Cost Basis",
    )

    ax.axhline(1200, color="black", linewidth=0.5, alpha=0.3)

    ax.annotate(
        f"${final_lump:,.2f}\n({lump_return:+.1f}%)",
        xy=(btc_prices.index[-1], final_lump),
        xytext=(10, 0),
        textcoords="offset points",
        fontsize=11,
        fontweight="bold",
        color="#8B5CF6",
        verticalalignment="center",
    )

    ax.annotate(
        f"${final_dca:,.2f}\n({dca_return:+.1f}%)",
        xy=(btc_prices.index[-1], final_dca),
        xytext=(10, 0),
        textcoords="offset points",
        fontsize=11,
        fontweight="bold",
        color="#0EA5E9",
        verticalalignment="center",
    )

    ax.set_title(
        "Bitcoin Investment Strategies: Lump Sum vs Dollar-Cost Averaging",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value (USD)")
    ax.legend(loc="upper left")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.xticks(rotation=45)

    winner = "DCA" if dca_return > lump_return else "Lump Sum"
    diff = abs(dca_return - lump_return)
    ax.text(
        0.98,
        0.02,
        f"Winner: {winner} (by {diff:.1f}%)",
        transform=ax.transAxes,
        fontsize=11,
        fontweight="bold",
        ha="right",
        va="bottom",
        bbox=dict(
            boxstyle="round", facecolor="#D1FAE5", edgecolor="#059669", alpha=0.9
        ),
    )

    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "dca_vs_lump_sum.png")
    plt.savefig(output_path, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")

    return lump_return, dca_return, final_lump, final_dca


def generate_q4_chart():
    print("Generating Q4: Volatility Comparison chart...")

    path = os.path.join(DATA_DIR, "intermediate/int_market__daily_returns.parquet")
    returns = pd.read_parquet(path)

    volatility = returns.groupby("asset_id")["daily_return"].std() * np.sqrt(252) * 100
    volatility = volatility.sort_values(ascending=False)

    categories = {
        "BTC": "Crypto",
        "AAPL": "Stock",
        "GOOGL": "Stock",
        "MSFT": "Stock",
        "SPY": "Index",
        "EUR": "Fiat",
        "GBP": "Fiat",
        "USD": "Fiat",
    }

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = [COLORS.get(asset, "#888888") for asset in volatility.index]
    bars = ax.bar(
        volatility.index,
        volatility.values,
        color=colors,
        alpha=0.85,
        edgecolor="white",
        linewidth=1.5,
    )

    for bar, val in zip(bars, volatility.values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.5,
            f"{val:.1f}%",
            ha="center",
            va="bottom",
            fontsize=11,
            fontweight="bold",
        )

    for bar, asset in zip(bars, volatility.index):
        cat = categories.get(asset, "")
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            -2.5,
            cat,
            ha="center",
            va="top",
            fontsize=9,
            color="gray",
        )

    btc_vol = volatility.get("BTC", 0)
    fiat_avg = volatility[["EUR", "GBP"]].mean()
    ratio = btc_vol / fiat_avg if fiat_avg > 0 else 0

    ax.set_title(
        "Annualized Volatility: Bitcoin vs Traditional Assets",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Asset")
    ax.set_ylabel("Annualized Volatility (%)")
    ax.set_ylim(0, max(volatility.values) * 1.15)

    textstr = f"BTC is {ratio:.1f}x more volatile\nthan major fiat currencies"
    props = dict(boxstyle="round", facecolor="#FEF3C7", alpha=0.9, edgecolor="#D97706")
    ax.text(
        0.98,
        0.98,
        textstr,
        transform=ax.transAxes,
        fontsize=11,
        verticalalignment="top",
        horizontalalignment="right",
        bbox=props,
    )

    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "volatility_comparison.png")
    plt.savefig(output_path, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")

    return volatility


def main():
    print("=" * 60)
    print("Generating Analysis Charts")
    print("=" * 60)
    print()

    q1_data = generate_q1_chart()
    q2_final_value, q2_return = generate_q2_chart()
    lump_return, dca_return, final_lump, final_dca = generate_q3_chart()
    volatility = generate_q4_chart()

    print()
    print("=" * 60)
    print("SUMMARY FOR REPORT UPDATE")
    print("=" * 60)
    print()
    print(f"Q2: $1K Investment Final Value: ${q2_final_value:,.2f} ({q2_return:+.2f}%)")
    print(f"Q3: Lump Sum: ${final_lump:,.2f} ({lump_return:+.2f}%)")
    print(f"Q3: DCA: ${final_dca:,.2f} ({dca_return:+.2f}%)")
    print(f"Q4: BTC Volatility: {volatility.get('BTC', 0):.2f}%")
    print(f"Q4: EUR Volatility: {volatility.get('EUR', 0):.2f}%")
    print(f"Q4: GBP Volatility: {volatility.get('GBP', 0):.2f}%")
    print()
    print("All charts generated successfully!")


if __name__ == "__main__":
    main()
