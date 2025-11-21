import type { NextAuthOptions } from 'next-auth';
import CredentialsProvider from 'next-auth/providers/credentials';
import {User} from '../../../types';
export const options: NextAuthOptions = {
    providers: [
        CredentialsProvider({
            name: "Credentials",
            credentials: {
                username: {
                    label: "Username:",
                    type: "text",
                    placeholder: "your user name"
                },   
                password: {
                    label: "Password:",
                    type: "password",
                    placeholder: "your password"
                },             
            },
            async authorize(credentials) {
                const symphonyApi = process.env.SYMPHONY_API;
                console.log("symphonyApi", symphonyApi);
            
                // Only send username and password
                const { username, password } = credentials ?? {};
                const res = await fetch(`${symphonyApi}users/auth`, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({ username, password })
                });
            
                const text = await res.text();
                console.log("API Response:", text);
            
                let user;
                try {
                    user = JSON.parse(text);
                } catch (err) {
                    console.error("JSON parse error:", err);
                    return null;
                }
            
                if (res.ok && user) {                    
                    return user;
                } 
                return null;
            }
            
        })
    ],
    callbacks: {
        async jwt({token, user}) {
            return {...token, ...user};
        },
        async session({session, token, user}) {
            session.user = token;
            return session;
        }
    }
}