import React from "react";

import { Header } from "./Header";

type User = {
  name: string;
};

export const Page: React.FC = () => {
  const [user, setUser] = React.useState<User>();

  return (
    <article>
      <Header
        user={user}
        onLogin={() => setUser({ name: "Jane Doe" })}
        onLogout={() => setUser(undefined)}
        onCreateAccount={() => setUser({ name: "Jane Doe" })}
      />

      <section className="mx-auto py-12 px-5 max-w-[600px] text-foreground text-sm leading-6 font-mono">
        <h2 className="inline-block align-top my-0 mb-1 font-bold text-3xl leading-none">
          Pages in Storybook
        </h2>
        <p className="my-4">
          We recommend building UIs with a{" "}
          <a
            className="text-inherit"
            href="https://componentdriven.org"
            target="_blank"
            rel="noopener noreferrer"
          >
            <strong>component-driven</strong>
          </a>{" "}
          process starting with atomic components and ending with pages.
        </p>
        <p className="my-4">
          Render pages with mock data. This makes it easy to build and review page states without
          needing to navigate to them in your app. Here are some handy patterns for managing page
          data in Storybook:
        </p>
        <ul className="my-4 pl-8">
          <li className="mb-2">
            Use a higher-level connected component. Storybook helps you compose such data from the
            "args" of child component stories
          </li>
          <li className="mb-2">
            Assemble data in the page component from your services. You can mock these services out
            using Storybook.
          </li>
        </ul>
        <p className="my-4">
          Get a guided tutorial on component-driven development at{" "}
          <a
            className="text-inherit"
            href="https://storybook.js.org/tutorials/"
            target="_blank"
            rel="noopener noreferrer"
          >
            Storybook tutorials
          </a>
          . Read more in the{" "}
          <a
            className="text-inherit"
            href="https://storybook.js.org/docs"
            target="_blank"
            rel="noopener noreferrer"
          >
            docs
          </a>
          .
        </p>
        <div className="mt-10 mb-10 text-xs leading-5">
          <span className="inline-block align-top mr-2.5 rounded-2xl bg-success-200 px-3 py-1 text-success-700 font-bold text-[11px] leading-3">
            Tip
          </span>
          Adjust the width of the canvas with the{" "}
          <svg
            className="inline-block align-top mt-0.5 mr-1 w-3 h-3"
            width="10"
            height="10"
            viewBox="0 0 12 12"
            xmlns="http://www.w3.org/2000/svg"
            aria-labelledby="viewports-icon-title"
          >
            <title id="viewports-icon-title">Viewports</title>
            <g fill="none" fillRule="evenodd">
              <path
                d="M1.5 5.2h4.8c.3 0 .5.2.5.4v5.1c-.1.2-.3.3-.4.3H1.4a.5.5 0 01-.5-.4V5.7c0-.3.2-.5.5-.5zm0-2.1h6.9c.3 0 .5.2.5.4v7a.5.5 0 01-1 0V4H1.5a.5.5 0 010-1zm0-2.1h9c.3 0 .5.2.5.4v9.1a.5.5 0 01-1 0V2H1.5a.5.5 0 010-1zm4.3 5.2H2V10h3.8V6.2z"
                id="a"
                fill="currentColor"
              />
            </g>
          </svg>
          Viewports addon in the toolbar
        </div>
      </section>
    </article>
  );
};
