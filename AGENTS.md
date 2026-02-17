Read ./README.md to understand the project goal.

## Chatting

If I ask you a question, don't automatically assume it's an implementation
request. Answer the question first and then ask if you should implement the
suggested solution.

## Coding tasks

- Ask me clarifying questions until you're 95% confident you can complete the
  task successfully
- Before asking any questions, make sure you can't infer the answer from the
  codebase
- Ask your questions one by one; don't combine multiple questions into a list
- Strive to formulate binary questions that can be answered yes or no
- If a binary question is impossible, provide an enumerated list of options so
  the answer can be a number
- If you think a task is too complex to implement in one go, suggest splitting
  it into subtasks, provide the subtask graph, and recommend which one to start
  with
- Write for humans - readability and simplicity are essential
- Ask before making any optimization that makes the code more complicated or
  less readable

### Rust code

After you're done with a Rust task, always run the following without asking a
permission:

Format the code:

```sh
cargo sort
cargo +nightly fmt
```

Make checks pass:

```sh
./.pre-commit.sh
```

#### Coding style

##### Deriving macros from external crates

- Use fully qualified paths, e.g. `serde::Derialize`
- Put external macros after built-in ones
- Sort them if possible

## Git

Never ask to stage or commit anything
